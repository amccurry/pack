package pack.block.blockstore.hdfs.blockstore;

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
import pack.block.blockstore.hdfs.blockstore.WalFile.Reader;
import pack.block.blockstore.hdfs.blockstore.WalFile.Writer;
import pack.block.blockstore.hdfs.file.WalKeyWritable;
import pack.block.blockstore.hdfs.file.WalKeyWritable.Type;

public class WalFileFactoryPackFileSyncTest {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./target/tmp/WalFileFactoryPackFileSyncTest");
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
  public void testWalFile() throws Exception {
    Path dir = new Path("/testWalFile");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA;
    WalFileFactoryPackFileSync file = new WalFileFactoryPackFileSync(fileSystem, metaData);
    Path src = new Path(dir, "wal.tmp");
    FSDataOutputStream out = fileSystem.create(src, false);

    WalKeyWritable key = new WalKeyWritable();
    key.setType(Type.DATA);
    BytesWritable value = new BytesWritable();
    key.write(out);
    value.write(out);
    key.write(out);
    out.hflush();

    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    Path dst = new Path(dir, "wal.dst");
    file.recover(src, dst);

    Reader reader = file.open(dst);
    if (!reader.next(key, value)) {
      fail();
    }
    if (reader.next(key, value)) {
      fail();
    }
  }

  @Test
  public void testWalFileWithTimeout() throws Exception {
    Path dir = new Path("/testWalFileWithTimeout");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .maxIdleWriterTime(TimeUnit.SECONDS.toNanos(1))
                                                          .build();

    WalFileFactoryPackFileSync file = new WalFileFactoryPackFileSync(fileSystem, metaData);
    Path src = new Path(dir, "wal.tmp");
    try (Writer writer = file.create(src)) {
      WalKeyWritable key = new WalKeyWritable();
      key.setType(Type.DATA);
      key.setStartingBlockId(1);
      key.setEtartingBlockId(2);
      BytesWritable value = new BytesWritable();

      writer.append(key, value);
      Thread.sleep(TimeUnit.SECONDS.toMillis(2));

      key.setStartingBlockId(2);
      key.setEtartingBlockId(3);
      writer.append(key, value);
    }

    WalKeyWritable key = new WalKeyWritable();
    BytesWritable value = new BytesWritable();
    Reader reader = file.open(src);
    if (!reader.next(key, value)) {
      fail();
    }
    if (!reader.next(key, value)) {
      fail();
    }
    if (reader.next(key, value)) {
      fail();
    }
  }
}
