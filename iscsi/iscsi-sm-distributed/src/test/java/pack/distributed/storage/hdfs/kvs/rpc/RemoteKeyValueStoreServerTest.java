package pack.distributed.storage.hdfs.kvs.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Closer;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.hdfs.kvs.BytesRef;
import pack.distributed.storage.minicluster.EmbeddedHdfsCluster;
import pack.distributed.storage.minicluster.EmbeddedTestUtils;
import pack.distributed.storage.minicluster.EmbeddedZookeeper;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;
import pack.iscsi.storage.utils.PackUtils;

public class RemoteKeyValueStoreServerTest {

  private static EmbeddedZookeeper ZOOKEEPER;
  private static EmbeddedHdfsCluster HDFS;
  private static Configuration CONF;
  private static String ZK_CONNECTION;

  @BeforeClass
  public static void setupClass() throws IOException {
    if (PackUtils.isPropertySet(PackConfig.ZK_CONNECTION)) {
      ZK_CONNECTION = PackConfig.getHdfsWalZooKeeperConnection();
    } else {
      ZOOKEEPER = new EmbeddedZookeeper();
      ZOOKEEPER.startup();
    }
    if (PackUtils.isPropertySet(PackConfig.HDFS_CONF_PATH)) {
      CONF = PackConfig.getConfiguration();
    } else {
      HDFS = new EmbeddedHdfsCluster();
      HDFS.startup();
    }
  }

  @AfterClass
  public static void teardownClass() {
    if (HDFS != null) {
      HDFS.shutdown();
    }
    if (ZOOKEEPER != null) {
      ZOOKEEPER.shutdown();
    }
  }

  @Test
  public void testRemoteKeyValueStoreServer() throws Exception {
    RemoteKeyValueStoreServer.setEmbeddedLookup(false);
    String connection = getZkConnection();
    ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper(connection, 30000);
    Configuration configuration = getConfiguration();
    RemoteKeyValueStoreServer server1;
    RemoteKeyValueStoreServer server2;
    try (Closer closer = Closer.create()) {
      Path rootKvs = new Path("/tmp/RemoteKeyValueStoreServerTest");
      FileSystem fileSystem = rootKvs.getFileSystem(configuration);
      fileSystem.delete(rootKvs, true);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String bindAddress = "127.0.0.1";

      Closer server1Closer = Closer.create();
      ZooKeeperClient zooKeeperS1 = server1Closer.register(ZkUtils.newZooKeeper(connection, 30000));
      server1 = server1Closer.register(RemoteKeyValueStoreServer.createInstance(bindAddress,
          EmbeddedTestUtils.getAvailablePort(), configuration, zooKeeperS1, rootKvs, ugi));
      server1.start();

      Closer server2Closer = Closer.create();
      ZooKeeperClient zooKeeperS2 = server2Closer.register(ZkUtils.newZooKeeper(connection, 30000));
      server2 = server2Closer.register(RemoteKeyValueStoreServer.createInstance(bindAddress,
          EmbeddedTestUtils.getAvailablePort(), configuration, zooKeeperS2, rootKvs, ugi));
      server2.start();

      printServerRunning("test1", zooKeeper);

      RemoteKeyValueStoreClient client = closer.register(RemoteKeyValueStoreClient.create(configuration, zooKeeper));
      runTestClient(client);

      server2Closer.close();

      printServerRunning("test1", zooKeeper);
      runTestClient(client);
      assertKvsIsOnServer("test1", zooKeeper, server1);

      server1Closer.close();

      printServerRunning("test1", zooKeeper);

      server2Closer = Closer.create();
      zooKeeperS2 = server2Closer.register(ZkUtils.newZooKeeper(connection, 30000));
      server2 = server2Closer.register(RemoteKeyValueStoreServer.createInstance(bindAddress,
          EmbeddedTestUtils.getAvailablePort(), configuration, zooKeeperS2, rootKvs, ugi));
      server2.start();

      printServerRunning("test1", zooKeeper);
      runTestClient(client);
      assertKvsIsOnServer("test1", zooKeeper, server2);

      server1Closer = Closer.create();
      zooKeeperS1 = server1Closer.register(ZkUtils.newZooKeeper(connection, 30000));
      server1 = server1Closer.register(RemoteKeyValueStoreServer.createInstance(bindAddress,
          EmbeddedTestUtils.getAvailablePort(), configuration, zooKeeperS1, rootKvs, ugi));
      server1.start();

      printServerRunning("test1", zooKeeper);
      runTestClient(client);

      server1Closer.close();
      server2Closer.close();

    }
  }

  @Test
  public void testRemoteKeyValueStoreServerContinous() throws Exception {
    RemoteKeyValueStoreServer.setEmbeddedLookup(false);
    String connection = getZkConnection();
    ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper(connection, 30000);
    Configuration configuration = getConfiguration();
    RemoteKeyValueStoreServer server1;
    RemoteKeyValueStoreServer server2;
    try (Closer closer = Closer.create()) {
      Path rootKvs = new Path("/tmp/RemoteKeyValueStoreServerTest");
      FileSystem fileSystem = rootKvs.getFileSystem(configuration);
      fileSystem.delete(rootKvs, true);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String bindAddress = "127.0.0.1";

      Closer server1Closer = Closer.create();
      ZooKeeperClient zooKeeperS1 = server1Closer.register(ZkUtils.newZooKeeper(connection, 30000));
      server1 = server1Closer.register(RemoteKeyValueStoreServer.createInstance(bindAddress,
          EmbeddedTestUtils.getAvailablePort(), configuration, zooKeeperS1, rootKvs, ugi));
      server1.start();

      Closer server2Closer = Closer.create();
      ZooKeeperClient zooKeeperS2 = server2Closer.register(ZkUtils.newZooKeeper(connection, 30000));
      server2 = server2Closer.register(RemoteKeyValueStoreServer.createInstance(bindAddress,
          EmbeddedTestUtils.getAvailablePort(), configuration, zooKeeperS2, rootKvs, ugi));
      server2.start();

      RemoteKeyValueStoreClient client = closer.register(RemoteKeyValueStoreClient.create(configuration, zooKeeper));

      long seed = 1;

      Thread threadWriter = new Thread(() -> {
        try {
          startWriter(client, "test2", 0, 200000, seed);
        } catch (IOException e) {
          e.printStackTrace();
          return;
        }
      });
      threadWriter.setDaemon(true);
      threadWriter.start();

      Thread threadReader = new Thread(() -> {
        try {
          startReader(client, "test2", 0, 200000, seed);
        } catch (IOException e) {
          e.printStackTrace();
          return;
        }
      });
      threadReader.setDaemon(true);
      threadReader.start();

      Thread.sleep(TimeUnit.SECONDS.toMillis(3));

      server1Closer.close();

      Thread.sleep(TimeUnit.SECONDS.toMillis(3));

      server1Closer = Closer.create();
      zooKeeperS1 = server1Closer.register(ZkUtils.newZooKeeper(connection, 30000));
      server1 = server1Closer.register(RemoteKeyValueStoreServer.createInstance(bindAddress,
          EmbeddedTestUtils.getAvailablePort(), configuration, zooKeeperS1, rootKvs, ugi));
      server1.start();

      Thread.sleep(TimeUnit.SECONDS.toMillis(3));

      server2Closer.close();

      server2Closer = Closer.create();
      zooKeeperS2 = server2Closer.register(ZkUtils.newZooKeeper(connection, 30000));
      server2 = server2Closer.register(RemoteKeyValueStoreServer.createInstance(bindAddress,
          EmbeddedTestUtils.getAvailablePort(), configuration, zooKeeperS2, rootKvs, ugi));
      server2.start();

      threadReader.join();
      threadWriter.join();

      server1Closer.close();
      server2Closer.close();

    }
  }

  private void startReader(RemoteKeyValueStoreClient client, String store, long start, long length, long seed)
      throws IOException {
    Random random = new Random(seed);
    long l = start;
    while (true) {
      ScanResult scanResult = client.scan(store, BytesReference.toBytesReference(BytesRef.value(l)));
      List<Pair<BytesRef, BytesRef>> result = scanResult.getResult();
      for (Pair<BytesRef, BytesRef> pair : result) {
        byte[] buf = new byte[4096];
        random.nextBytes(buf);

        BytesRef key = pair.getT1();
        long keyLong = PackUtils.getLong(key.bytes, key.offset);

        assertTrue(Arrays.equals(buf, pair.getT2().bytes));
        l = keyLong + 1;
      }
      if (!result.isEmpty()) {
        client.deleteRange(store, BytesReference.toBytesReference(BytesRef.value(0)),
            BytesReference.toBytesReference(BytesRef.value(l - 1)));
      }
      if (l >= start + length) {
        return;
      }
    }
  }

  private void startWriter(RemoteKeyValueStoreClient client, String store, long start, long length, long seed)
      throws IOException {
    Random random = new Random(seed);
    for (long l = start; l < length; l++) {
      byte[] buf = new byte[4096];
      random.nextBytes(buf);
      client.put(store, BytesReference.toBytesReference(BytesRef.value(l)),
          BytesReference.toBytesReference(new BytesRef(buf)));
    }
  }

  private void assertKvsIsOnServer(String name, ZooKeeperClient zooKeeper, RemoteKeyValueStoreServer server1)
      throws KeeperException, InterruptedException {
    String path = RemoteKeyValueStoreServer.STORES + "/" + name;
    Stat stat = zooKeeper.exists(path, false);
    assertNotNull(stat);
    byte[] data = zooKeeper.getData(path, false, stat);
    assertEquals(server1.getServerAddress(), new String(data));
  }

  private void printServerRunning(String name, ZooKeeperClient zooKeeper) throws KeeperException, InterruptedException {
    String path = RemoteKeyValueStoreServer.STORES + "/" + name;
    Stat stat = zooKeeper.exists(path, false);
    if (stat != null) {
      byte[] data = zooKeeper.getData(path, false, stat);
      System.out.println("======================");
      System.out.println(new String(data));
    }
  }

  private void runTestClient(RemoteKeyValueStoreClient client) throws IOException {
    BytesReference key = BytesReference.toBytesReference(BytesRef.value(123L));
    BytesReference value = BytesReference.toBytesReference(new BytesRef("test1"));
    TransId transId = client.put("test1", key, value);
    client.sync("test1", transId);
    GetResult result = client.get("test1", key);
    assertTrue(result.isFound());
    assertEquals("test1", result.getValue()
                                .utf8ToString());
  }

  private String getZkConnection() {
    if (ZOOKEEPER == null) {
      return ZK_CONNECTION;
    } else {
      return ZOOKEEPER.getConnection();
    }
  }

  private Configuration getConfiguration() throws IOException {
    if (HDFS == null) {
      return CONF;
    } else {
      return HDFS.getFileSystem()
                 .getConf();
    }
  }

}
