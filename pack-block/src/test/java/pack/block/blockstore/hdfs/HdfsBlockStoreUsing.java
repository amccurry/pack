package pack.block.blockstore.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.fuse.FuseFS;

public class HdfsBlockStoreUsing {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./test");
  private static FileSystem fileSystem;

  public static void main(String[] args) throws IOException {
    // System.out.println(33555456 % 4096);
    // System.out.println(260096 % 4096);

    Configuration configuration = new Configuration();
    String storePath = storePathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();
    Path path = new Path("/test");
    int fileSystemBlockSize = HdfsBlockStoreConfig.DEFAULT_CONFIG.getFileSystemBlockSize();
    HdfsMetaData metaData = HdfsMetaData.builder()
                                        .length((100000000000L / fileSystemBlockSize) * fileSystemBlockSize)
                                        .build();
    HdfsBlockStore.writeHdfsMetaData(metaData, fileSystem, path);
    try (FuseFS memfs = new FuseFS("./mnt")) {
      memfs.addBlockStore(new HdfsBlockStore(fileSystem, path));
      // memfs.addBlockStore(new FileBlockStore(new File("data/data1")));
      memfs.localMount();
    }
  }

}
