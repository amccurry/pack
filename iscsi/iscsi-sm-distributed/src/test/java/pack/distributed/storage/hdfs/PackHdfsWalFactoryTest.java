package pack.distributed.storage.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.minicluster.EmbeddedHdfsCluster;
import pack.distributed.storage.minicluster.EmbeddedTestUtils;
import pack.distributed.storage.minicluster.EmbeddedZookeeper;
import pack.distributed.storage.wal.PackWalFactory;
import pack.distributed.storage.wal.PackWalFactoryTestBase;
import pack.iscsi.storage.utils.PackUtils;

public class PackHdfsWalFactoryTest extends PackWalFactoryTestBase {

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
      ZK_CONNECTION = ZOOKEEPER.getConnection();
    }
    if (PackUtils.isPropertySet(PackConfig.HDFS_CONF_PATH)) {
      CONF = PackConfig.getConfiguration();
    } else {
      HDFS = new EmbeddedHdfsCluster();
      HDFS.startup();
      CONF = HDFS.getFileSystem()
                 .getConf();
    }
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    if (HDFS != null) {
      HDFS.shutdown();
    }
    if (ZOOKEEPER != null) {
      ZOOKEEPER.shutdown();
    }
  }

  @Override
  protected PackWalFactory createPackWalFactory() throws Exception {
    String hdfsWalBindAddress = "127.0.0.1";
    int hdfsWalPort = EmbeddedTestUtils.getAvailablePort();
    Path hdfsWalDir = new Path("/tmp/PackHdfsWalFactoryTest");
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return new PackHdfsWalFactory(CONF, ZK_CONNECTION, 30000, hdfsWalBindAddress, hdfsWalPort, hdfsWalDir, ugi);
  }

}
