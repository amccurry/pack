package pack.distributed.storage.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import pack.distributed.storage.minicluster.EmbeddedHdfsCluster;
import pack.distributed.storage.wal.PackWalFactory;
import pack.distributed.storage.wal.PackWalFactoryTestBase;

public class PackHdfsWalFactoryTest extends PackWalFactoryTestBase {

  private static EmbeddedHdfsCluster HDFS;

  @BeforeClass
  public static void setupClass() throws IOException {
    HDFS = new EmbeddedHdfsCluster();
    HDFS.startup();
  }

  @AfterClass
  public static void teardownClass() {
    HDFS.shutdown();
  }

  @Override
  protected PackWalFactory createPackWalFactory() throws Exception {
    Path rootWal = new Path("/wal");
    return new PackHdfsWalFactory(HDFS.getFileSystem()
                                      .getConf(),
        rootWal);
  }

}
