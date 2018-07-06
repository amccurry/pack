package pack.block.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import pack.zk.utils.ZkMiniCluster;
import pack.zk.utils.ZkUtils;
import pack.zk.utils.ZooKeeperClient;
import pack.zk.utils.ZooKeeperClientFactory;
import pack.zk.utils.ZooKeeperLockManager;

public class UtilsTest {

  @Test
  public void testZooKeeperSessionTimeout() throws KeeperException, InterruptedException, IOException {

    String name = "test123";

    ZkMiniCluster zkMiniCluster = new ZkMiniCluster();
    zkMiniCluster.startZooKeeper(true, new File("target/ZkUtilsTest").getAbsolutePath());
    try {
      int sessionTimeout = 5000;
      String zkConnectionString = zkMiniCluster.getZkConnectionString();

      ZooKeeperClientFactory zkcf = ZkUtils.newZooKeeperClientFactory(zkConnectionString, sessionTimeout);
      ZkUtils.mkNodesStr(zkcf.getZk(), "/test");
      ZooKeeperLockManager lockManager = ZkUtils.newZooKeeperLockManager(zkcf, "/test");

      for (int i = 0; i < 3; i++) {
        if (lockManager.tryToLock(name)) {
          zkMiniCluster.shutdownZooKeeper();
          Thread.sleep(6000);
          zkMiniCluster.startZooKeeper(false, new File("target/ZkUtilsTest").getAbsolutePath());
          // kinda weird but ZK hasn't cleaned up old locks yet so the count can
          // be variable
          lockManager.getNumberOfLockNodesPresent(name);
          lockManager.unlock(name);
        }
      }
    } finally {
      zkMiniCluster.shutdownZooKeeper();
    }
  }

}
