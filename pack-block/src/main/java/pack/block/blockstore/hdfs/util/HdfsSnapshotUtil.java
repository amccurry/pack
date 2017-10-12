package pack.block.blockstore.hdfs.util;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsSnapshotUtil {

  private static final String HDFS = "hdfs";
  private static final String YYYYMMDDKKMMSS = "yyyyMMddkkmmssSSS";
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsSnapshotUtil.class);

  public static void main(String[] args) {
    System.out.println(getMountSnapshotName());
  }

  public static void createSnapshot(FileSystem fileSystem, Path path, String snapshotName)
      throws IOException, InterruptedException {
    createSnapshot(fileSystem, path, snapshotName, getHdfsProxy());
  }

  public static void createSnapshot(FileSystem fileSystem, Path path, String snapshotName, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      dfs.allowSnapshot(path);
      dfs.createSnapshot(path, snapshotName);
      return null;
    });
  }

  public static String getMountSnapshotName() {
    String now = new SimpleDateFormat(YYYYMMDDKKMMSS).format(new Date());
    return ".mount." + now;
  }

  public static void cleanupOldMountSnapshots(FileSystem fileSystem, Path path, int maxNumberOfMountSnapshots)
      throws IOException, InterruptedException {
    cleanupOldMountSnapshots(fileSystem, path, maxNumberOfMountSnapshots, getHdfsProxy());
  }

  private static UserGroupInformation getHdfsProxy() throws IOException {
    return UserGroupInformation.createProxyUser(HDFS, UserGroupInformation.getCurrentUser());
  }

  public static void cleanupOldMountSnapshots(FileSystem fileSystem, Path path, int maxNumberOfMountSnapshots,
      UserGroupInformation ugi) throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      FileStatus[] listStatus = dfs.listStatus(new Path(path, ".snapshot"));
      Arrays.sort(listStatus, Collections.reverseOrder());
      for (int i = maxNumberOfMountSnapshots; i < listStatus.length; i++) {
        String name = listStatus[i].getPath()
                                   .getName();
        LOGGER.info("Removing old snapshot {} {}", path, name);
        dfs.deleteSnapshot(path, name);
      }
      return null;
    });
  }

}
