package pack.distributed.storage.zk;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.Test;

import com.google.common.base.Joiner;

import pack.distributed.storage.minicluster.EmbeddedTestUtils;
import pack.iscsi.storage.utils.PackUtils;

public class PackZooKeeperClusterTest {

  @Test
  public void testPackZooKeeperClusterTest()
      throws IOException, ConfigException, KeeperException, InterruptedException {
    File root = new File("./target/test/PackZooKeeperClusterTest");
    PackUtils.rmr(root);

    List<String> serverList = new ArrayList<>();
    List<PackZooKeeperCluster> cluster = new ArrayList<>();
    List<PackZooKeeperServerConfig> configs = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      configs.add(PackZooKeeperServerConfig.builder()
                                           .hostname("localhost")
                                           .clientPort(EmbeddedTestUtils.getAvailablePort())
                                           .peerPort(EmbeddedTestUtils.getAvailablePort())
                                           .leaderElectPort(EmbeddedTestUtils.getAvailablePort())
                                           .id(i)
                                           .build());
    }

    for (PackZooKeeperServerConfig config : configs) {
      File dir = new File(root, "zk-" + config.getId());
      cluster.add(new PackZooKeeperCluster(dir, config, configs));
      serverList.add(config.getHostname() + ":" + config.getClientPort());
    }

    String connectionString = Joiner.on(',')
                                    .join(serverList);
    try (ZooKeeperClient zk = ZkUtils.newZooKeeper(connectionString, 30000)) {
      String path = zk.create("/test", "test".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      Stat stat = zk.exists(path, false);
      assertNotNull(stat);
      byte[] data = zk.getData("/test", false, stat);
      assertEquals("test", new String(data));
    }

    for (PackZooKeeperCluster keeperCluster : cluster) {
      keeperCluster.close();
    }
  }

}
