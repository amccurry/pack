package pack.block.blockstore.hdfs.util;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.util.Utils;

public class HdfsSnapshotUtil {

  private static final String SNAPSHOT = ".snapshot";
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsSnapshotUtil.class);

  public static void enableSnapshots(FileSystem fileSystem, Path path) throws IOException, InterruptedException {
    enableSnapshots(fileSystem, path, Utils.getUserGroupInformation());
  }

  public static void createSnapshot(FileSystem fileSystem, Path path, String snapshotName)
      throws IOException, InterruptedException {
    createSnapshot(fileSystem, path, snapshotName, Utils.getUserGroupInformation());
  }

  public static String getMountSnapshotName(HdfsSnapshotStrategy strategy) {
    return getMountSnapshotName(strategy, new Date());
  }

  public static String getMountSnapshotName(HdfsSnapshotStrategy strategy, Date date) {
    return strategy.getMountSnapshotName(date);
  }

  public static String getMountSnapshotName(HdfsSnapshotStrategy strategy, long timestamp) {
    return getMountSnapshotName(strategy, new Date(timestamp));
  }

  public static void cleanupOldMountSnapshots(FileSystem fileSystem, Path path, HdfsSnapshotStrategy strategy)
      throws IOException, InterruptedException {
    cleanupOldMountSnapshots(fileSystem, path, Utils.getUserGroupInformation(), strategy);
  }

  public static void removeAllSnapshots(FileSystem fileSystem, Path path) throws IOException, InterruptedException {
    removeAllSnapshots(fileSystem, path, Utils.getUserGroupInformation());
  }

  public static void disableSnapshots(FileSystem fileSystem, Path path) throws IOException, InterruptedException {
    disableSnapshots(fileSystem, path, Utils.getUserGroupInformation());
  }

  public static void enableSnapshots(FileSystem fileSystem, Path path, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
    LOGGER.info("Using ugi {} to enable volume snapshots", ugi);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      if (dfs.exists(new Path(path, SNAPSHOT))) {
        return null;
      }
      dfs.allowSnapshot(path);
      return null;
    });
  }

  public static void createSnapshot(FileSystem fileSystem, Path path, String snapshotName, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;

    LOGGER.info("Using ugi {} to create volume snapshots", ugi);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      if (!dfs.exists(new Path(path, SNAPSHOT))) {
        LOGGER.info("Can not create snapshot {}", snapshotName);
        return null;
      }
      LOGGER.info("Created snapshot {}", dfs.createSnapshot(path, snapshotName));
      return null;
    });
  }

  public static void cleanupOldMountSnapshots(FileSystem fileSystem, Path path, UserGroupInformation ugi,
      HdfsSnapshotStrategy strategy) throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      FileStatus[] listStatus = dfs.listStatus(new Path(path, SNAPSHOT));
      Set<String> currentSnapshots = new TreeSet<>();
      for (FileStatus fileStatus : listStatus) {
        String name = fileStatus.getPath()
                                .getName();
        currentSnapshots.add(name);
      }

      Collection<String> snapshotsToRemove = strategy.getSnapshotsToRemove(currentSnapshots);
      for (String snapshot : snapshotsToRemove) {
        LOGGER.info("Removing old snapshot {} {}", path, snapshot);
        dfs.deleteSnapshot(path, snapshot);
      }
      return null;
    });
  }

  public static void removeAllSnapshots(FileSystem fileSystem, Path path, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;

    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      if (!dfs.exists(new Path(path, SNAPSHOT))) {
        return null;
      }
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

  public static void disableSnapshots(FileSystem fileSystem, Path path, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;

    LOGGER.info("Using ugi {} to disable volume snapshots", ugi);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      if (!dfs.exists(new Path(path, SNAPSHOT))) {
        return null;
      }
      dfs.disallowSnapshot(path);
      return null;
    });
  }
}
