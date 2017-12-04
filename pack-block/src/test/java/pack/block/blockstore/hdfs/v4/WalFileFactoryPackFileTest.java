package pack.block.blockstore.hdfs.v4;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BytesWritable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.file.WalKeyWritable;
import pack.block.blockstore.hdfs.file.WalKeyWritable.Type;
import pack.block.blockstore.hdfs.v4.WalFile.Reader;

public class WalFileFactoryPackFileTest {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./target/tmp/WalFileFactoryPackFileTest");
  private static FileSystem fileSystem;

  @BeforeClass
  public static void beforeClass() throws IOException {
    Configuration configuration = new Configuration();
    String storePath = new File(storePathDir, "hdfs").getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();
  }

  @AfterClass
  public static void afterClass() {
    cluster.shutdown();
  }

  @Test
  public void testServer() throws Exception {
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA;
    WalFileFactoryPackFile file = new WalFileFactoryPackFile(fileSystem, metaData);
    Path src = new Path("/wal.tmp");
    FSDataOutputStream out = fileSystem.create(src, false);

    WalKeyWritable key = new WalKeyWritable();
    key.setType(Type.DATA);
    BytesWritable value = new BytesWritable();
    key.write(out);
    value.write(out);
    key.write(out);
    out.hflush();

    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    Path dst = new Path("/wal.dst");
    file.recover(src, dst);

    Reader reader = file.open(dst);
    if (!reader.next(key, value)) {
      fail();
    }
    if (reader.next(key, value)) {
      fail();
    }
  }
}
