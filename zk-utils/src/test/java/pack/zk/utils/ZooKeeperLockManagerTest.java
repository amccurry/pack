package pack.zk.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZooKeeperLockManagerTest {
  private static ZkMiniCluster _zkMiniCluster;
  private static ExecutorService _service;

  @BeforeClass
  public static void setupZookeeper() {
    _zkMiniCluster = new ZkMiniCluster();
    _zkMiniCluster.startZooKeeper(new File("target/ZooKeeperLockManagerTest").getAbsolutePath(), true);
    _service = Executors.newCachedThreadPool();
  }

  @AfterClass
  public static void tearDownZookeeper() {
    _service.shutdownNow();
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
    try (ZooKeeperClient zk1 = ZkUtils.newZooKeeper(_zkMiniCluster.getZkConnectionString(), 10000)) {
      try (ZooKeeperLockManager manager = ZkUtils.newZooKeeperLockManager(zk1, "/lock")) {
        try (ZooKeeperClient zk2 = ZkUtils.newZooKeeper(_zkMiniCluster.getZkConnectionString(), 10000)) {
          try (ZooKeeperLockManager manager2 = ZkUtils.newZooKeeperLockManager(zk2, "/lock")) {
            assertTrue(manager.tryToLock("test"));
            assertFalse(manager.tryToLock("test"));

            Future<Void> future = run(() -> {
              manager2.lock("test");
              return null;
            });

            Thread.sleep(3000);

            assertFalse(future.isDone());

            manager.unlock("test");

            Thread.sleep(3000);

            assertTrue(future.isDone());
          }
        }
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
    List<String> locks = new ArrayList<>(Arrays.asList("a_9999999999", "a_-9999999999", "a_-1"));
    ZooKeeperLockManager.orderLocks(locks);

    assertEquals("a_9999999999", locks.get(0));
    assertEquals("a_-9999999999", locks.get(1));
    assertEquals("a_-1", locks.get(2));
  }

  @Test
  public void testOrderingLocksMixZero() {
    List<String> locks = new ArrayList<>(Arrays.asList("a_0", "a_-1", "a_1"));
    ZooKeeperLockManager.orderLocks(locks);
    
    assertEquals("a_-1", locks.get(0));
  }

  private <T> Future<T> run(Callable<T> callable) {
    return _service.submit(callable);
  }

}
