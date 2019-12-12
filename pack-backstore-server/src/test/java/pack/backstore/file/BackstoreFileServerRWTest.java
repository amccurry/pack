package pack.backstore.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.backstore.file.client.BackstoreFileServiceClient;
import pack.backstore.file.client.BackstoreFileServiceClientConfig;
import pack.backstore.file.client.BackstoreFileServiceClientConfig.BackstoreFileServiceClientConfigBuilder;
import pack.backstore.file.server.BackstoreFileServerConfig;
import pack.backstore.file.server.BackstoreFileServerRW;
import pack.backstore.file.thrift.generated.BackstoreError;
import pack.backstore.file.thrift.generated.BackstoreFileServiceException;
import pack.backstore.file.thrift.generated.CreateFileRequest;
import pack.backstore.file.thrift.generated.DestroyFileRequest;
import pack.backstore.file.thrift.generated.ExistsFileRequest;
import pack.backstore.file.thrift.generated.ReadFileRequest;
import pack.backstore.file.thrift.generated.ReadFileRequestBatch;
import pack.backstore.file.thrift.generated.ReadFileResponse;
import pack.backstore.file.thrift.generated.ReadFileResponseBatch;
import pack.backstore.file.thrift.generated.WriteFileRequest;
import pack.backstore.file.thrift.generated.WriteFileRequestBatch;
import pack.util.IOUtils;

public class BackstoreFileServerRWTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BackstoreFileServerRWTest.class);

  private final File _baseDir = new File("target/tmp/BackstoreFileServerRWTest");
  private BackstoreFileServerRW _server;

  private File _storeDir;

  @Before
  public void setup() throws Exception {
    _storeDir = new File(_baseDir, UUID.randomUUID()
                                       .toString());
    BackstoreFileServerConfig config = BackstoreFileServerConfig.builder()
                                                                .port(0)
                                                                .storeDir(_storeDir)
                                                                .build();
    _server = new BackstoreFileServerRW(config);
    _server.start(false);
    Thread.sleep(TimeUnit.SECONDS.toMillis(2));
  }

  @After
  public void teardown() {
    IOUtils.close(LOGGER, _server);
    IOUtils.rmr(_storeDir);
  }

  @Test
  public void testCreateAndDestroy() throws Exception {
    String filename = UUID.randomUUID()
                          .toString();
    int length = 16 * 1024 * 1024;
    try (BackstoreFileServiceClient client = getClient()) {
      assertFalse(client.exists(new ExistsFileRequest(filename))
                        .isExists());

      client.create(new CreateFileRequest(filename, length));

      assertTrue(client.exists(new ExistsFileRequest(filename))
                       .isExists());

      client.destroy(new DestroyFileRequest(filename));

      assertFalse(client.exists(new ExistsFileRequest(filename))
                        .isExists());
    }
  }

  @Test
  public void testReadsAndWrites() throws Exception {
    String filename = UUID.randomUUID()
                          .toString();
    int length = 16 * 1024 * 1024;
    try (BackstoreFileServiceClient client = getClient()) {
      client.create(new CreateFileRequest(filename, length));
      assertTrue(client.exists(new ExistsFileRequest(filename))
                       .isExists());

      {
        List<WriteFileRequest> writeRequests = new ArrayList<>();
        writeRequests.add(new WriteFileRequest(0, ByteBuffer.wrap(new byte[] { 1, 2, 3 })));
        writeRequests.add(new WriteFileRequest(4, ByteBuffer.wrap(new byte[] { 3, 2, 1 })));

        WriteFileRequestBatch request = new WriteFileRequestBatch();
        request.setFilename(filename);
        request.setWriteRequests(writeRequests);

        client.write(request);
      }
      {
        List<ReadFileRequest> readRequests = new ArrayList<>();
        readRequests.add(new ReadFileRequest(0, 7));
        readRequests.add(new ReadFileRequest(4, 3));

        ReadFileRequestBatch request = new ReadFileRequestBatch();
        request.setFilename(filename);
        request.setReadRequests(readRequests);
        ReadFileResponseBatch response = client.read(request);
        List<ReadFileResponse> responses = response.getReadResponses();
        {
          ReadFileResponse readFileResponse = responses.get(0);
          byte[] data = readFileResponse.getData();
          assertTrue(Arrays.equals(new byte[] { 1, 2, 3, 0, 3, 2, 1 }, data));
        }
        {
          ReadFileResponse readFileResponse = responses.get(1);
          byte[] data = readFileResponse.getData();
          assertTrue(Arrays.equals(new byte[] { 3, 2, 1 }, data));
        }
      }

      client.destroy(new DestroyFileRequest(filename));
      assertFalse(client.exists(new ExistsFileRequest(filename))
                        .isExists());
    }
  }

  @Test
  public void testFileNotFoundError() throws Exception {
    String filename = UUID.randomUUID()
                          .toString();
    try (BackstoreFileServiceClient client = getClient()) {
      {
        List<WriteFileRequest> writeRequests = new ArrayList<>();
        writeRequests.add(new WriteFileRequest(0, ByteBuffer.wrap(new byte[] { 1, 2, 3 })));

        WriteFileRequestBatch request = new WriteFileRequestBatch();
        request.setFilename(filename);
        request.setWriteRequests(writeRequests);

        try {
          client.write(request);
          fail();
        } catch (BackstoreFileServiceException e) {
          assertEquals(BackstoreError.FILE_NOT_FOUND, e.getErrorType());
        }
      }
      {
        List<ReadFileRequest> readRequests = new ArrayList<>();
        readRequests.add(new ReadFileRequest(0, 7));

        ReadFileRequestBatch request = new ReadFileRequestBatch();
        request.setFilename(filename);
        request.setReadRequests(readRequests);
        try {
          client.read(request);
        } catch (BackstoreFileServiceException e) {
          assertEquals(BackstoreError.FILE_NOT_FOUND, e.getErrorType());
        }
      }
    }
  }

  private BackstoreFileServiceClient getClient() throws IOException {
    BackstoreFileServiceClientConfigBuilder builder = BackstoreFileServiceClientConfig.builder();
    String hostName = _server.getBindInetAddress()
                             .getHostName();
    int bindPort = _server.getBindPort();
    BackstoreFileServiceClientConfig config = builder.hostname(hostName)
                                                     .port(bindPort)
                                                     .build();
    return BackstoreFileServiceClient.create(config);
  }

}
