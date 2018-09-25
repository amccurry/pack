package pack.block.blockstore.hdfs.lock;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class PackLockFactory {

  public static boolean isLocked(Configuration configuration, Path lockPath) throws IOException, InterruptedException {
    FileSystem fileSystem = lockPath.getFileSystem(configuration);
    if (fileSystem instanceof DistributedFileSystem) {
      return HdfsLock.isLocked(configuration, lockPath);
    }
    return FileLock.isLocked(lockPath.toUri()
                                     .getPath());
  }

  public static PackLock create(Configuration configuration, Path lockPath, LockLostAction lockLostAction)
      throws IOException, InterruptedException {
    FileSystem fileSystem = lockPath.getFileSystem(configuration);
    if (fileSystem instanceof DistributedFileSystem) {
      return new HdfsLock(configuration, lockPath, lockLostAction);
    }
    return new FileLock(lockPath.toUri()
                                .getPath());
  }

}
