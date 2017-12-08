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

import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImpl;
import pack.block.fuse.FuseFileSystem;

public class HdfsBlockStoreUsing {

  private static MiniDFSCluster cluster;
  private static File storeBasePathDir = new File("./target/test");
  private static File storeHdfsPathDir = new File(storeBasePathDir, "hdfs");
  private static File storeCachePathDir = new File(storeBasePathDir, "cache");
  private static FileSystem fileSystem;
  private static MetricRegistry metrics = new MetricRegistry();

  public static void main(String[] args) throws IOException {
    Configuration configuration = new Configuration();
    String storePath = storeHdfsPathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();

    File file = new File("./tmp-conf/");
    file.mkdirs();
    writeConfiguration(new File(file, "hdfs-site.xml"));

    Path path = new Path("/test");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, path);
    try (FuseFileSystem memfs = new FuseFileSystem("./mnt")) {
      memfs.addBlockStore(new HdfsBlockStoreImpl(metrics, storeCachePathDir, fileSystem, path));
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
