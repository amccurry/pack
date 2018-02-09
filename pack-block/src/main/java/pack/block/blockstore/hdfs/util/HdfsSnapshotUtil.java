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

  private static final String SNAPSHOT = ".snapshot";
  private static final String HDFS = "hdfs";
  private static final String PACK_HDFS_SUPER_USER = "PACK_HDFS_SUPER_USER";
  private static final String YYYYMMDDKKMMSS = "yyyyMMddkkmmssSSS";
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsSnapshotUtil.class);

  public static void main(String[] args) {
    System.out.println(getMountSnapshotName());
  }

  public static void enableSnapshots(FileSystem fileSystem, Path path) throws IOException, InterruptedException {
    enableSnapshots(fileSystem, path, getUgi());
  }

  public static void enableSnapshots(FileSystem fileSystem, Path path, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
    if (dfs.exists(new Path(path, SNAPSHOT))) {
      return;
    }
    LOGGER.info("Using ugi {} to enable volume snapshots", ugi);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      dfs.allowSnapshot(path);
      return null;
    });
  }

  public static void createSnapshot(FileSystem fileSystem, Path path, String snapshotName)
      throws IOException, InterruptedException {
    createSnapshot(fileSystem, path, snapshotName, getUgi());
  }

  public static void createSnapshot(FileSystem fileSystem, Path path, String snapshotName, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
    if (dfs.exists(new Path(path, SNAPSHOT))) {
      return;
    }
    LOGGER.info("Using ugi {} to create volume snapshots", ugi);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
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
    cleanupOldMountSnapshots(fileSystem, path, maxNumberOfMountSnapshots, getUgi());
  }

  public static UserGroupInformation getUgi() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      return UserGroupInformation.getCurrentUser();
    } else {
      String superUser = System.getenv(PACK_HDFS_SUPER_USER);
      if (superUser == null) {
        superUser = HDFS;
      }
      return UserGroupInformation.createRemoteUser(superUser);
    }
  }

  public static void cleanupOldMountSnapshots(FileSystem fileSystem, Path path, int maxNumberOfMountSnapshots,
      UserGroupInformation ugi) throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      FileStatus[] listStatus = dfs.listStatus(new Path(path, SNAPSHOT));
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

  public static void removeAllSnapshots(FileSystem fileSystem, Path path) throws IOException, InterruptedException {
    removeAllSnapshots(fileSystem, path, getUgi());
  }

  public static void removeAllSnapshots(FileSystem fileSystem, Path path, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      FileStatus[] listStatus = dfs.listStatus(new Path(path, SNAPSHOT));
      for (FileStatus fileStatus : listStatus) {
        String name = fileStatus.getPath()
                                .getName();
        LOGGER.info("Removing old snapshot {} {}", path, name);
        dfs.deleteSnapshot(path, name);
      }
      return null;
    });
  }

  public static void disableSnapshots(FileSystem fileSystem, Path path) throws IOException, InterruptedException {
    removeSnapshot(fileSystem, path, getUgi());
  }

  public static void removeSnapshot(FileSystem fileSystem, Path path, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
    if (dfs.exists(new Path(path, SNAPSHOT))) {
      return;
    }
    LOGGER.info("Using ugi {} to disable volume snapshots", ugi);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      dfs.disallowSnapshot(path);
      return null;
    });
  }
}
