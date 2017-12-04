package pack.zk.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
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
    try (ZooKeeperLockManager manager = ZkUtils.newZooKeeperLockManager(_zkMiniCluster.getZkConnectionString(), 10000,
        "/lock")) {
      try (ZooKeeperLockManager manager2 = ZkUtils.newZooKeeperLockManager(_zkMiniCluster.getZkConnectionString(),
          10000, "/lock")) {
        assertTrue(manager.tryToLock("test"));
        assertFalse(manager.tryToLock("test"));

        Future<Void> future = run(() -> {
          manager2.lock("test");
          return null;
        });

        Thread.sleep(1000);

        assertFalse(future.isDone());

        manager.unlock("test");

        Thread.sleep(1000);

        assertTrue(future.isDone());
      }
    }
  }

  private <T> Future<T> run(Callable<T> callable) {
    return _service.submit(callable);
  }

}
