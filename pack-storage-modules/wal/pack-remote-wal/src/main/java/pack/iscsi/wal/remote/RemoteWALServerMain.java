package pack.iscsi.wal.remote;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.wal.remote.RemoteWALServer.RemoteWriteAheadLogServerConfig;

public class RemoteWALServerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteWALServerMain.class);

  public static void main(String[] args) throws Exception {
    CommandLine cmd = RemoteWALServerArgsUtil.parseArgs(args);
    String zkConnection = RemoteWALServerArgsUtil.getZkConnection(cmd);
    String zkPrefix = RemoteWALServerArgsUtil.getZkPrefix(cmd);
    File walLogDir = RemoteWALServerArgsUtil.getWalLogDir(cmd);
    String jaegerEndpoint = RemoteWALServerArgsUtil.getJaegerEndpoint(cmd);
    if (jaegerEndpoint != null) {
      RemoteWALTracerConfig.setupTracer(jaegerEndpoint);
    }

    RetryPolicy retryPolicy = new RetryForever((int) TimeUnit.SECONDS.toMillis(10));
    CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zkConnection, retryPolicy);
    curatorFramework.getUnhandledErrorListenable()
                    .addListener((message, e) -> {
                      LOGGER.error("Unknown error " + message, e);
                    });
    curatorFramework.getConnectionStateListenable()
                    .addListener((c, newState) -> {
                      LOGGER.info("Connection state {}", newState);
                    });
    curatorFramework.start();
    Runtime.getRuntime()
           .addShutdownHook(new Thread(() -> curatorFramework.close()));
    RemoteWriteAheadLogServerConfig config = RemoteWriteAheadLogServerConfig.builder()
                                                                            .walLogDir(walLogDir)
                                                                            .curatorFramework(curatorFramework)
                                                                            .zkPrefix(zkPrefix)
                                                                            .port(0)
                                                                            .build();
    try (RemoteWALServer server = new RemoteWALServer(config)) {
      LOGGER.info("Starting server");
      server.start(true);
    }
  }
}
