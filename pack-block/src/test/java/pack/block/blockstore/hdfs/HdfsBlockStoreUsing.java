package pack.block.blockstore.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import com.codahale.metrics.MetricRegistry;

import pack.block.blockstore.hdfs.v1.HdfsBlockStoreV1;
import pack.block.fuse.FuseFileSystem;

public class HdfsBlockStoreUsing {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./test");
  private static FileSystem fileSystem;
  private static MetricRegistry metrics = new MetricRegistry();

  public static void main(String[] args) throws IOException {
    Configuration configuration = new Configuration();
    String storePath = storePathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();

    File file = new File("./tmp-conf/");
    file.mkdirs();
    writeConfiguration(new File(file, "hdfs-site.xml"));

    Path path = new Path("/test");
    long length = 32 * 1024 * 1024;
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          // .length(length)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, path);
    try (FuseFileSystem memfs = new FuseFileSystem("./mnt")) {
      memfs.addBlockStore(new HdfsBlockStoreV1(metrics, fileSystem, path));
      // {
      // File file2 = new File("data/data1");
      // try (RandomAccessFile rand = new RandomAccessFile(file2, "rw")) {
      // rand.setLength(length);
      // }
      // memfs.addBlockStore(new FileBlockStore(file2));
      // }
      memfs.localMount();
    }
  }

  private static void writeConfiguration(File file) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      Configuration conf = fileSystem.getConf();
      conf.writeXml(outputStream);
    }
  }

}
