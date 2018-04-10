package pack.distributed.storage.zk;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import pack.distributed.storage.minicluster.EmbeddedZookeeper;
import pack.distributed.storage.wal.PackWalFactory;
import pack.distributed.storage.wal.PackWalFactoryTestBase;

public class PackZooKeeperWalFactoryTest extends PackWalFactoryTestBase {

  private static EmbeddedZookeeper ZOOKEEPER;

  @BeforeClass
  public static void setupClass() throws IOException {
    ZOOKEEPER = new EmbeddedZookeeper();
    ZOOKEEPER.startup();
  }

  @AfterClass
  public static void teardownClass() {
    ZOOKEEPER.shutdown();
  }

  @Override
  protected PackWalFactory createPackWalFactory() throws Exception {
    String connection = ZOOKEEPER.getConnection();
    ZooKeeperClient zk = _closer.register(ZkUtils.newZooKeeper(connection, 30000));
    return new PackZooKeeperWalFactory(zk);
  }

}
