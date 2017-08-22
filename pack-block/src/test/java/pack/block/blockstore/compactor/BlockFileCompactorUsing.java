package pack.block.blockstore.compactor;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import pack.block.blockstore.hdfs.HdfsBlockStore;

public class BlockFileCompactorUsing {

  public static void main(String[] args) throws IOException {

    Configuration configuration = new Configuration(true);
    configuration.addResource(new FileInputStream("./tmp-conf/hdfs-site.xml"));

    FileSystem fileSystem = FileSystem.get(configuration);
    Path path = new Path("/block/testing1");
    {
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus fileStatus : listStatus) {
        System.out.println(fileStatus.getPath());
      }
    }
    {
      FileStatus[] listStatus = fileSystem.listStatus(new Path(path, HdfsBlockStore.BLOCK));
      for (FileStatus fileStatus : listStatus) {
        System.out.println(fileStatus.getPath() + " " + fileStatus.getLen());
      }
    }

//    BlockFileCompactor compactor = new BlockFileCompactor(fileSystem, path);
//    compactor.runCompaction();
  }

}
