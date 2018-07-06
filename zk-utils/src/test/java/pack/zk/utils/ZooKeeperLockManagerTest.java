package pack.zk.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZooKeeperLockManagerTest {
  private static ZkMiniCluster _zkMiniCluster;

  @BeforeClass
  public static void setupZookeeper() {
    _zkMiniCluster = new ZkMiniCluster();
    _zkMiniCluster.startZooKeeper(new File("target/ZooKeeperLockManagerTest").getAbsolutePath(), 0);
  }

  @AfterClass
  public static void tearDownZookeeper() {
    _zkMiniCluster.shutdownZooKeeper();
  }

  @Before
  public void setup() throws IOException {
    try (ZooKeeperClient zk = ZkUtils.newZooKeeper(_zkMiniCluster.getZkConnectionString(), 10000)) {
      ZkUtils.mkNodes(zk, "/lock");
    }
  }

  @Test
  public void testLockManager() throws IOException, KeeperException, InterruptedException {
    ZooKeeperClientFactory zk1 = ZkUtils.newZooKeeperClientFactory(_zkMiniCluster.getZkConnectionString(), 10000);
    ZooKeeperClientFactory zk2 = ZkUtils.newZooKeeperClientFactory(_zkMiniCluster.getZkConnectionString(), 10000);
    try (ZooKeeperLockManager manager = ZkUtils.newZooKeeperLockManager(zk1, "/lock")) {
      try (ZooKeeperLockManager manager2 = ZkUtils.newZooKeeperLockManager(zk2, "/lock")) {
        assertTrue(manager.tryToLock("test"));
        assertFalse(manager2.tryToLock("test"));
      }
    }
  }

  @Test
  public void testOrderingLocksSimple() {
    List<String> locks = new ArrayList<>(Arrays.asList("a_10", "a_2", "a_3"));
    ZooKeeperLockManager.orderLocks(locks);

    assertEquals("a_2", locks.get(0));
    assertEquals("a_3", locks.get(1));
    assertEquals("a_10", locks.get(2));
  }

  @Test
  public void testOrderingLocksNegative() {
    List<String> locks = new ArrayList<>(Arrays.asList("a_-10", "a_-2", "a_-3"));
    ZooKeeperLockManager.orderLocks(locks);

    assertEquals("a_-10", locks.get(0));
    assertEquals("a_-3", locks.get(1));
    assertEquals("a_-2", locks.get(2));
  }

  @Test
  public void testOrderingLocksMix() {
    List<Entry<String, Long>> orderLocksWithVersion = Arrays.asList(ZooKeeperLockManager.newEntry("a_9999999999", 100),
        ZooKeeperLockManager.newEntry("a_-9999999999", 200), ZooKeeperLockManager.newEntry("a_-1", 300));
    List<String> locks = ZooKeeperLockManager.orderLocksWithVersion(orderLocksWithVersion);

    assertEquals("a_9999999999", locks.get(0));
    assertEquals("a_-9999999999", locks.get(1));
    assertEquals("a_-1", locks.get(2));
  }

  @Test
  public void testOrderingLocksMixZero() {
    {
      List<String> locks = new ArrayList<>(Arrays.asList("a_0", "a_-1", "a_1"));
      try {
        ZooKeeperLockManager.orderLocks(locks);
      } catch (Exception e) {

      }
    }

    List<Entry<String, Long>> orderLocksWithVersion = Arrays.asList(ZooKeeperLockManager.newEntry("a_-1", 100),
        ZooKeeperLockManager.newEntry("a_0", 200), ZooKeeperLockManager.newEntry("a_1", 300));
    List<String> locks = ZooKeeperLockManager.orderLocksWithVersion(orderLocksWithVersion);

    assertEquals("a_-1", locks.get(0));
    assertEquals("a_0", locks.get(1));
    assertEquals("a_1", locks.get(2));
  }

}
