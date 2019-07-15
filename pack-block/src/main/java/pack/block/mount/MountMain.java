package pack.block.mount;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

import jnr.ffi.Pointer;
import jnrfuse.ErrorCodes;
import jnrfuse.FuseFillDir;
import jnrfuse.FuseStubFS;
import jnrfuse.flags.FallocFlags;
import jnrfuse.struct.FileStat;
import jnrfuse.struct.FuseFileInfo;
import pack.block.blockserver.BlockServer;
import pack.block.blockserver.local.LocalBlockServer;
import pack.block.fuse.FuseFileSystemSingleMount;

public class MountMain extends FuseStubFS implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FuseFileSystemSingleMount.class);

  private static final String NONEMPTY = "nonempty";
  private static final String SYNC = "sync";
  private static final String PARENT_DIR = "..";
  private static final String CURRENT_DIR = ".";
  private static final String FILE_SEP = "/";
  private static final String OPTION_SWITCH = "-o";
  private static final String AUTO_UNMOUNT = "auto_unmount";
  private static final String ALLOW_ROOT = "allow_root";
  private static final Splitter PATH_SEP_SPLITTER = Splitter.on(FILE_SEP);

  private final String _localPath;
  private final UserGroupInformation _ugi;
  private final long _retryDelay = TimeUnit.SECONDS.toMillis(1);
  private final BlockServer _blockServer;
  private final AtomicBoolean _running = new AtomicBoolean(true);

  public static void main(String[] args) throws IOException {
    try (MountMain mm = new MountMain(UserGroupInformation.getCurrentUser(), "./mount")) {
      mm.localMount();
    }
  }

  public MountMain(UserGroupInformation ugi, String localPath) {
    _ugi = ugi;
    _localPath = localPath;
    _blockServer = new LocalBlockServer(new File("./bs"));
  }

  private BlockServer getBlockServer() {
    return _blockServer;
  }

  public void localMount() {
    try {
      localMount(true);
    } catch (Throwable t) {
      LOGGER.error("Unknown error during fuse mount", t);
    }
  }

  public void localMount(boolean blocking) {
    jnr.ffi.Platform p = jnr.ffi.Platform.getNativePlatform();
    String[] opts;
    switch (p.getOS()) {
    case DARWIN:
      opts = new String[] { OPTION_SWITCH, ALLOW_ROOT };
      break;
    case LINUX:
    default:
      opts = new String[] { OPTION_SWITCH, ALLOW_ROOT, OPTION_SWITCH, AUTO_UNMOUNT, OPTION_SWITCH, SYNC, OPTION_SWITCH,
          NONEMPTY };
      break;
    }
    mount(Paths.get(_localPath), blocking, true, opts);
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("close");
    umount();
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    if (isValidVolumeSnapshotPath(path)) {
      String volumeName = getVolumeNameFromPath(path);
      String snapshotId = getSnapshotIdFromPath(path);
      long mountId;
      try {
        mountId = retryUntilSuccessful(() -> {
          BlockServer blockServer = getBlockServer();
          return blockServer.mount(volumeName, snapshotId);
        });
      } catch (Exception e) {
        LOGGER.error("Unknown error", e);
        return -ErrorCodes.EIO();
      }
      fi.fh.set(mountId);
      return 0;
    } else {
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    long mountId = fi.fh.get();
    try {
      retryUntilSuccessful(() -> {
        BlockServer blockServer = getBlockServer();
        blockServer.unmount(mountId);
        return null;
      });
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return -ErrorCodes.EIO();
    }
    return super.release(path, fi);
  }

  @Override
  public int readdir(String path, Pointer buf, FuseFillDir filter, long offset, FuseFileInfo fi) {
    try {
      if (isRootPath(path)) {
        List<String> volumes = retryUntilSuccessful(() -> {
          BlockServer blockServer = getBlockServer();
          return blockServer.volumes();
        });
        filter.apply(buf, CURRENT_DIR, null, 0);
        filter.apply(buf, PARENT_DIR, null, 0);
        for (String volume : volumes) {
          filter.apply(buf, volume, null, 0);
        }
        return 0;
      } else if (isValidVolumePath(path)) {
        String volumeName = getVolumeNameFromPath(path);
        List<String> snapshotIds = retryUntilSuccessful(() -> {
          BlockServer blockServer = getBlockServer();
          return blockServer.snapshots(volumeName);
        });
        filter.apply(buf, CURRENT_DIR, null, 0);
        filter.apply(buf, PARENT_DIR, null, 0);
        for (String snapshotId : snapshotIds) {
          filter.apply(buf, snapshotId, null, 0);
        }
        return 0;
      } else {
        return -ErrorCodes.ENOTDIR();
      }
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int getattr(String path, FileStat stat) {
    try {
      stat.st_mtim.tv_sec.set(System.currentTimeMillis() / 1000);
      stat.st_mtim.tv_nsec.set(0);
      if (isRootPath(path)) {
        stat.st_mode.set(FileStat.S_IFDIR | 0755);
        stat.st_size.set(2);
        return 0;
      } else if (isValidVolumePath(path)) {
        String volumeName = getVolumeNameFromPath(path);
        if (retryUntilSuccessful(isValidVolume(volumeName))) {
          List<String> snapshots = retryUntilSuccessful(getVolumes());
          stat.st_mode.set(FileStat.S_IFDIR | 0755);
          stat.st_size.set(snapshots.size());
          return 0;
        } else {
          return -ErrorCodes.ENOENT();
        }
      } else if (isValidVolumeSnapshotPath(path)) {
        String volumeName = getVolumeNameFromPath(path);
        String snapshotId = getSnapshotIdFromPath(path);
        if (retryUntilSuccessful(isValidSnapshot(volumeName, snapshotId))) {
          long size = retryUntilSuccessful(getSnapshotSize(volumeName, snapshotId));
          stat.st_mode.set(FileStat.S_IFREG | 0755);
          stat.st_size.set(size);
          return 0;
        } else {
          return -ErrorCodes.ENOENT();
        }
      } else {
        return -ErrorCodes.ENOENT();
      }
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return -ErrorCodes.EIO();
    }
  }

  private PrivilegedExceptionAction<Long> getSnapshotSize(String volumeName, String snapshotId) {
    return () -> {
      BlockServer blockServer = getBlockServer();
      return blockServer.getSnapshotLength(volumeName, snapshotId);
    };
  }

  private PrivilegedExceptionAction<Boolean> isValidVolume(String volumeName) {
    return () -> {
      BlockServer blockServer = getBlockServer();
      return blockServer.volumes()
                        .contains(volumeName);
    };
  }

  private PrivilegedExceptionAction<List<String>> getVolumes() {
    return () -> {
      BlockServer blockServer = getBlockServer();
      return blockServer.volumes();
    };
  }

  private PrivilegedExceptionAction<Boolean> isValidSnapshot(String volumeName, String snapshotId) {
    return () -> {
      BlockServer blockServer = getBlockServer();
      if (blockServer.volumes()
                     .contains(volumeName)) {
        return blockServer.snapshots(volumeName)
                          .contains(snapshotId);
      } else {
        return false;
      }
    };
  }

  @Override
  public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
    long mountId = fi.fh.get();
    try {
      int intSize = (int) size;
      byte[] data = retryUntilSuccessful(() -> {
        BlockServer blockServer = getBlockServer();
        return blockServer.read(mountId, offset, intSize);
      });
      buf.put(0, data, 0, intSize);
      return intSize;
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
    long mountId = fi.fh.get();
    try {
      int intSize = (int) size;
      byte[] data = new byte[intSize];
      buf.get(0, data, 0, intSize);
      retryUntilSuccessful(() -> {
        BlockServer blockServer = getBlockServer();
        blockServer.write(mountId, offset, data);
        return null;
      });
      buf.put(0, data, 0, intSize);
      return intSize;
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int fallocate(String path, int mode, long off, long length, FuseFileInfo fi) {
    long mountId = fi.fh.get();
    try {
      Set<FallocFlags> lookup = FallocFlags.lookup(mode);
      if (lookup.contains(FallocFlags.FALLOC_FL_PUNCH_HOLE) && lookup.contains(FallocFlags.FALLOC_FL_KEEP_SIZE)) {
        retryUntilSuccessful(() -> {
          BlockServer blockServer = getBlockServer();
          blockServer.delete(mountId, off, (int) length);
          return null;
        });
      }
      return 0;
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return -ErrorCodes.EIO();
    }
  }

  private String getSnapshotIdFromPath(String path) {
    List<String> list = PATH_SEP_SPLITTER.splitToList(path);
    return list.get(2);
  }

  private String getVolumeNameFromPath(String path) {
    List<String> list = PATH_SEP_SPLITTER.splitToList(path);
    return list.get(1);
  }

  private boolean isRootPath(String path) {
    return FILE_SEP.equals(path);
  }

  private boolean isValidVolumePath(String path) {
    List<String> list = PATH_SEP_SPLITTER.splitToList(path);
    return list.size() == 2 && !list.get(1)
                                    .isEmpty();
  }

  private boolean isValidVolumeSnapshotPath(String path) {
    List<String> list = PATH_SEP_SPLITTER.splitToList(path);
    return list.size() == 3 && !list.get(1)
                                    .isEmpty()
        && !list.get(2)
                .isEmpty();
  }

  private <T> T retryUntilSuccessful(PrivilegedExceptionAction<T> action) throws Exception {
    while (_running.get()) {
      try {
        return _ugi.doAs(action);
      } catch (IOException | InterruptedException e) {
        LOGGER.error("Unknown error", e);
      }
      Thread.sleep(_retryDelay);
    }
    throw new RuntimeException("Not running");
  }
}
