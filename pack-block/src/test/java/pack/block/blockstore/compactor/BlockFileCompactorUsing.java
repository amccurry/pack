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

import pack.block.blockstore.BlockStoreMetaData;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImplConfig;

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
        Path path = new Path(args[0]);
        {
          FileStatus[] listStatus = fileSystem.listStatus(path);
          for (FileStatus fileStatus : listStatus) {
            System.out.println(fileStatus.getPath());
          }
        }
        {
          FileStatus[] listStatus = fileSystem.listStatus(new Path(path, HdfsBlockStoreImplConfig.BLOCK));
          for (FileStatus fileStatus : listStatus) {
            System.out.println(fileStatus.getPath() + " " + fileStatus.getLen());
          }
        }
        ContentSummary contentSummary = fileSystem.getContentSummary(path);
        System.out.println(contentSummary.getLength());

        long maxBlockFileSize = 1_000_000_000;

        BlockStoreMetaData newMetaData = BlockStoreMetaData.DEFAULT_META_DATA.toBuilder()
                                                                 .maxBlockFileSize(maxBlockFileSize)
                                                                 .maxObsoleteRatio(-0.1)
                                                                 .build();

        try (BlockFileCompactor compactor = new BlockFileCompactor(fileSystem, path, newMetaData)) {
          compactor.runCompaction(() -> true);
        }
        return null;
      }
    });

  }

}
