package pack.distributed.storage.zk;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackZooKeeperCluster implements Closeable {

  private static Logger LOGGER = LoggerFactory.getLogger(PackZooKeeperCluster.class);

  private static final String MYID = "myid";
  private final Thread _serverThread;
  private final PackQuorumPeerMain _quorumPeerMain;
  private final int _clientPort;

  public PackZooKeeperCluster(File dir, PackZooKeeperServerConfig myConfig, List<PackZooKeeperServerConfig> configs)
      throws IOException, ConfigException {
    _clientPort = myConfig.getClientPort();
    dir.mkdirs();
    writeMyIdIfNeeded(dir, myConfig.getId());
    Properties zkProp = new Properties();
    zkProp.setProperty("tickTime", "2000");
    zkProp.setProperty("initLimit", "10");
    zkProp.setProperty("syncLimit", "5");
    zkProp.setProperty("dataDir", dir.getAbsolutePath());
    zkProp.setProperty("dataLogDir", dir.getAbsolutePath());
    zkProp.setProperty("clientPort", Integer.toString(myConfig.getClientPort()));
    zkProp.setProperty("maxClientCnxns", "120");
    zkProp.setProperty("minSessionTimeout", "4000");
    zkProp.setProperty("maxSessionTimeout", "40000");
    zkProp.setProperty("leaderServes", "yes");
    for (PackZooKeeperServerConfig config : configs) {
      zkProp.setProperty("server." + config.getId(),
          config.getHostname() + ":" + config.getPeerPort() + ":" + config.getLeaderElectPort());
    }

    QuorumPeerConfig config = new QuorumPeerConfig();
    config.parseProperties(zkProp);

    _quorumPeerMain = new PackQuorumPeerMain();
    _serverThread =

        createServerThread(config);
    _serverThread.start();
  }

  public String getLocalConnection() {
    return "localhost:" + _clientPort;
  }

  @Override
  public void close() throws IOException {
    QuorumPeer quorumPeer = _quorumPeerMain.getQuorumPeer();
    ServerCnxnFactory factory = quorumPeer.getCnxnFactory();
    factory.shutdown();
  }

  private void writeMyIdIfNeeded(File dataDir, long myid) throws IOException {
    File myIdFile = new File(dataDir, MYID);
    if (!myIdFile.exists()) {
      try (PrintWriter writer = new PrintWriter(myIdFile)) {
        writer.println(Long.toString(myid));
      }
    }
  }

  private Thread createServerThread(QuorumPeerConfig config) {
    Thread thread = new Thread(() -> {
      try {
        _quorumPeerMain.runFromConfig(config);
      } catch (Exception e) {
        LOGGER.error("Unknown error", e);
      }
    });
    thread.setDaemon(true);
    thread.setName("zookeeper-main");
    return thread;
  }

  static class PackQuorumPeerMain extends QuorumPeerMain {
    QuorumPeer getQuorumPeer() {
      return this.quorumPeer;
    }
  }

}
