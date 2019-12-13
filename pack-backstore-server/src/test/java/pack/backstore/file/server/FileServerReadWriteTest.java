package pack.backstore.file.server;

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

import pack.backstore.file.client.FileServiceClient;
import pack.backstore.file.client.FileServiceClientConfig;
import pack.backstore.file.client.FileServiceClientConfig.FileServiceClientConfigBuilder;
import pack.backstore.thrift.generated.BackstoreError;
import pack.backstore.thrift.generated.BackstoreServiceException;
import pack.backstore.thrift.generated.CreateFileRequest;
import pack.backstore.thrift.generated.DestroyFileRequest;
import pack.backstore.thrift.generated.ExistsFileRequest;
import pack.backstore.thrift.generated.ReadFileRequest;
import pack.backstore.thrift.generated.ReadFileRequestBatch;
import pack.backstore.thrift.generated.ReadFileResponse;
import pack.backstore.thrift.generated.ReadFileResponseBatch;
import pack.backstore.thrift.generated.WriteFileRequest;
import pack.backstore.thrift.generated.WriteFileRequestBatch;
import pack.thrift.common.ClientFactory;
import pack.util.IOUtils;

public class FileServerReadWriteTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileServerReadWriteTest.class);

  private final File _baseDir = new File("target/tmp/BackstoreFileServerRWTest");
  private FileServerReadWrite _server;

  private File _storeDir;

  @Before
  public void setup() throws Exception {
    _storeDir = new File(_baseDir, UUID.randomUUID()
                                       .toString());
    FileServerConfig config = getFileServerConfig();
    _server = new FileServerReadWrite(config);
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
    try (FileServiceClient client = getFileServiceClient()) {
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
    try (FileServiceClient client = getFileServiceClient()) {
      client.create(new CreateFileRequest(filename, length));
      assertTrue(client.exists(new ExistsFileRequest(filename))
                       .isExists());
      String lockId = getLockId(filename);
      {
        List<WriteFileRequest> writeRequests = new ArrayList<>();
        writeRequests.add(new WriteFileRequest(0, ByteBuffer.wrap(new byte[] { 1, 2, 3 })));
        writeRequests.add(new WriteFileRequest(4, ByteBuffer.wrap(new byte[] { 3, 2, 1 })));

        WriteFileRequestBatch request = new WriteFileRequestBatch();
        request.setFilename(filename);
        request.setWriteRequests(writeRequests);
        request.setLockId(lockId);

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
      releaseLockId(filename, lockId);
      assertFalse(client.exists(new ExistsFileRequest(filename))
                        .isExists());
    }
  }

  protected void releaseLockId(String filename, String lockId) throws Exception {

  }

  protected String getLockId(String filename) throws Exception {
    return null;
  }

  @Test
  public void testFileNotFoundError() throws Exception {
    String filename = UUID.randomUUID()
                          .toString();
    try (FileServiceClient client = getFileServiceClient()) {
      {
        List<WriteFileRequest> writeRequests = new ArrayList<>();
        writeRequests.add(new WriteFileRequest(0, ByteBuffer.wrap(new byte[] { 1, 2, 3 })));

        WriteFileRequestBatch request = new WriteFileRequestBatch();
        request.setFilename(filename);
        request.setWriteRequests(writeRequests);

        try {
          client.write(request);
          fail();
        } catch (BackstoreServiceException e) {
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
        } catch (BackstoreServiceException e) {
          assertEquals(BackstoreError.FILE_NOT_FOUND, e.getErrorType());
        }
      }
    }
  }

  protected FileServiceClient getFileServiceClient() throws IOException {
    FileServiceClientConfigBuilder builder = FileServiceClientConfig.builder();
    String hostName = _server.getBindInetAddress()
                             .getHostName();
    int bindPort = _server.getBindPort();
    FileServiceClientConfig config = builder.hostname(hostName)
                                            .port(bindPort)
                                            .build();
    return ClientFactory.create(config);
  }

  protected FileServerConfig getFileServerConfig() {
    return FileServerConfig.builder()
                           .port(0)
                           .storeDir(_storeDir)
                           .build();
  }
}
