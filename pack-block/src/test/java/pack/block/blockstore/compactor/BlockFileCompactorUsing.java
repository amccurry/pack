package pack.block.blockstore.compactor;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;

public class BlockFileCompactorUsing {

  public static void main(String[] args) throws IOException, InterruptedException {

    Configuration configuration = new Configuration(false);
    configuration.addResource(new FileInputStream("./hadoop-conf/core-site.xml"));
    configuration.addResource(new FileInputStream("./hadoop-conf/hdfs-site.xml"));

    UserGroupInformation.setConfiguration(configuration);

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();

    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/user/sigma/block-volumes/testing");
        {
          FileStatus[] listStatus = fileSystem.listStatus(path);
          for (FileStatus fileStatus : listStatus) {
            System.out.println(fileStatus.getPath());
          }
        }
        {
          FileStatus[] listStatus = fileSystem.listStatus(new Path(path, HdfsBlockStoreConfig.BLOCK));
          for (FileStatus fileStatus : listStatus) {
            System.out.println(fileStatus.getPath() + " " + fileStatus.getLen());
          }
        }
        ContentSummary contentSummary = fileSystem.getContentSummary(path);
        System.out.println(contentSummary.getLength());

        long maxBlockFileSize = 1_000_000_000;
        try (BlockFileCompactor compactor = new BlockFileCompactor(fileSystem, path, maxBlockFileSize, -0.1, null)) {
          compactor.runCompaction();
        }
        return null;
      }
    });

  }

}
