package pack.backstore.coordinator.server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.backstore.coordinator.client.CoordinatorServiceClient;
import pack.backstore.coordinator.client.CoordinatorServiceClientConfig;
import pack.backstore.coordinator.client.CoordinatorServiceClientConfig.CoordinatorServiceClientConfigBuilder;
import pack.backstore.coordinator.server.CoordinatorServerConfig.CoordinatorServerConfigBuilder;
import pack.backstore.file.server.FileServerConfig;
import pack.backstore.file.server.FileServerReadWriteTest;
import pack.backstore.thrift.common.ClientFactory;
import pack.backstore.thrift.generated.RegisterFileRequest;
import pack.backstore.thrift.generated.RegisterFileResponse;
import pack.backstore.thrift.generated.ReleaseFileRequest;
import pack.util.IOUtils;

public class CoordinatorServerTest extends FileServerReadWriteTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileServerReadWriteTest.class);

  private CoordinatorServer _coordinatorServer;

  @Before
  public void setup() throws Exception {
    CoordinatorServerConfigBuilder builder = CoordinatorServerConfig.builder();
    CoordinatorServerConfig config = builder.port(0)
                                            .build();
    _coordinatorServer = new CoordinatorServer(config);
    _coordinatorServer.start(false);
    Thread.sleep(TimeUnit.SECONDS.toMillis(2));
    super.setup();
  }

  @After
  public void teardown() {
    super.teardown();
    IOUtils.close(LOGGER, _coordinatorServer);
  }

  @Override
  protected FileServerConfig getFileServerConfig() {
    String hostName = _coordinatorServer.getBindInetAddress()
                                        .getHostName();
    int bindPort = _coordinatorServer.getBindPort();
    CoordinatorServiceClientConfig config = CoordinatorServiceClientConfig.builder()
                                                                          .hostname(hostName)
                                                                          .port(bindPort)
                                                                          .build();
    CoordinatorLockIdManager lockIdManager = new CoordinatorLockIdManager(config);
    return super.getFileServerConfig().toBuilder()
                                      .lockIdManager(lockIdManager)
                                      .build();
  }

  @Override
  protected void releaseLockId(String filename, String lockId) throws Exception {
    try (CoordinatorServiceClient client = getCoordinatorServiceClient()) {
      client.releaseFileLock(new ReleaseFileRequest(filename, lockId));
    }
  }

  @Override
  protected String getLockId(String filename) throws Exception {
    try (CoordinatorServiceClient client = getCoordinatorServiceClient()) {
      RegisterFileResponse response = client.registerFileLock(new RegisterFileRequest(filename));
      return response.getLockId();
    }
  }

  protected CoordinatorServiceClient getCoordinatorServiceClient() throws IOException {
    CoordinatorServiceClientConfigBuilder builder = CoordinatorServiceClientConfig.builder();
    String hostName = _coordinatorServer.getBindInetAddress()
                                        .getHostName();
    int bindPort = _coordinatorServer.getBindPort();
    CoordinatorServiceClientConfig config = builder.hostname(hostName)
                                                   .port(bindPort)
                                                   .build();
    return ClientFactory.create(config);
  }

}
