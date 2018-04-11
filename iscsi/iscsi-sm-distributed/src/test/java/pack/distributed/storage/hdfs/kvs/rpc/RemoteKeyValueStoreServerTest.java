package pack.distributed.storage.hdfs.kvs.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

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

      server1 = RemoteKeyValueStoreServer.createInstance(bindAddress, EmbeddedTestUtils.getAvailablePort(),
          configuration, zooKeeper, rootKvs, ugi);
      server1.start();

      server2 = RemoteKeyValueStoreServer.createInstance(bindAddress, EmbeddedTestUtils.getAvailablePort(),
          configuration, zooKeeper, rootKvs, ugi);
      server2.start();

      printServerRunning("test1", zooKeeper);

      RemoteKeyValueStoreClient client = closer.register(RemoteKeyValueStoreClient.create(configuration, zooKeeper));
      runTestClient(client);

      server2.close();

      printServerRunning("test1", zooKeeper);
      runTestClient(client);
      assertKvsIsOnServer("test1", zooKeeper, server1);

      server1.close();

      printServerRunning("test1", zooKeeper);

      server2 = RemoteKeyValueStoreServer.createInstance(bindAddress, EmbeddedTestUtils.getAvailablePort(),
          configuration, zooKeeper, rootKvs, ugi);
      server2.start();

      printServerRunning("test1", zooKeeper);
      runTestClient(client);
      assertKvsIsOnServer("test1", zooKeeper, server2);

      server1 = RemoteKeyValueStoreServer.createInstance(bindAddress, EmbeddedTestUtils.getAvailablePort(),
          configuration, zooKeeper, rootKvs, ugi);
      server1.start();

      printServerRunning("test1", zooKeeper);
      runTestClient(client);

      server1.close();
      server2.close();

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
    Key key = Key.toKey(BytesRef.value(123L));
    Key value = Key.toKey(new BytesRef("test1"));
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
