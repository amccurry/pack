package pack.block.blockstore.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import pack.block.fuse.FuseFS;

public class HdfsBlockStoreUsing {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./test");
  private static FileSystem fileSystem;

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
    int fileSystemBlockSize = HdfsBlockStoreConfig.DEFAULT_CONFIG.getFileSystemBlockSize();
    HdfsMetaData metaData = HdfsMetaData.builder()
                                        .length((1000000000000L / fileSystemBlockSize) * fileSystemBlockSize)
                                        .build();
    HdfsBlockStore.writeHdfsMetaData(metaData, fileSystem, path);
    try (FuseFS memfs = new FuseFS("./mnt")) {
      memfs.addBlockStore(new HdfsBlockStore(fileSystem, path));
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
