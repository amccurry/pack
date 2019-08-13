package pack.s3;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import jnr.ffi.Pointer;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnrfuse.ErrorCodes;
import jnrfuse.FuseFillDir;
import jnrfuse.FuseStubFS;
import jnrfuse.struct.FileStat;
import jnrfuse.struct.FuseFileInfo;
import pack.s3.thrift.PackService;

public class PackV3 extends FuseStubFS implements Closeable {

  private static final String VOLUME = "volume";
  private static final String VOLUME_PREFIX = "/" + VOLUME + "/";
  private static final String STAT = "stat";
  private static final String STAT_PREFIX = "/" + STAT + "/";
  private static final String MOUNT = "mount";
  private static final String MOUNT_PREFIX = "/" + MOUNT + "/";
  private static final String BIG_WRITES = "big_writes";

  private static final Logger LOGGER = LoggerFactory.getLogger(PackV3.class);

  private static final String NONEMPTY = "nonempty";
  private static final String SYNC = "sync";
  private static final String PARENT_DIR = "..";
  private static final String CURRENT_DIR = ".";
  private static final String OPTION_SWITCH = "-o";
  private static final String AUTO_UNMOUNT = "auto_unmount";
  private static final String ALLOW_ROOT = "allow_root";
  private static final int OK = 0;

  private final String _localPath;

  private final boolean _sync = true;
  private final boolean _bigWrites = true;
  private final String _domain;

  public static void main(String[] args) throws Exception {
    try (PackV3 pack = new PackV3(args[0], args[1])) {
      pack.localMount();
    }
  }

  public PackV3(String localPath, String domain) throws Exception {
    mkdir(localPath);
    _localPath = localPath;
    _domain = domain;
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("close");
    umount();
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
    default: {
      Builder<String> builder = ImmutableList.builder();
      if (_sync) {
        builder.add(OPTION_SWITCH)
               .add(SYNC);
      }
      if (_bigWrites) {
        builder.add(OPTION_SWITCH)
               .add(BIG_WRITES);
      }
      builder.add(OPTION_SWITCH)
             .add(ALLOW_ROOT)
             .add(OPTION_SWITCH)
             .add(AUTO_UNMOUNT)
             .add(OPTION_SWITCH)
             .add(NONEMPTY);
      opts = builder.build()
                    .toArray(new String[] {});
      break;
    }
    }
    mount(Paths.get(_localPath), blocking, false, opts);
  }

  public PackService.Iface getClient() {
    return null;
  }

  @Override
  public int getattr(String path, FileStat stat) {
    try {
      if (isRoot(path)) {
        stat.st_mode.set(FileStat.S_IFDIR | 0755);
        stat.st_size.set(3);
        stat.st_mtim.tv_sec.set(System.currentTimeMillis() / 1000);
        stat.st_mtim.tv_nsec.set(0);
        return OK;
      } else if (isVolumeRoot(path)) {
        List<String> volumes = getClient().volumes();
        stat.st_mode.set(FileStat.S_IFDIR | 0755);
        stat.st_size.set(volumes.size());
        stat.st_mtim.tv_sec.set(System.currentTimeMillis() / 1000);
        stat.st_mtim.tv_nsec.set(0);
        return OK;
      } else if (isStatRoot(path)) {
        stat.st_mode.set(FileStat.S_IFDIR | 0755);
        stat.st_size.set(0);
        stat.st_mtim.tv_sec.set(System.currentTimeMillis() / 1000);
        stat.st_mtim.tv_nsec.set(0);
        return OK;
      } else if (isMountRoot(path)) {
        List<String> mounts = getCurrentMounts();
        stat.st_mode.set(FileStat.S_IFDIR | 0755);
        stat.st_size.set(mounts.size());
        stat.st_mtim.tv_sec.set(System.currentTimeMillis() / 1000);
        stat.st_mtim.tv_nsec.set(0);
        return OK;
      } else if (isVolumePath(path)) {
        String volumeName = getVolumeName(path);
        long length = getClient().sizeVolume(volumeName);
        stat.st_mode.set(FileStat.S_IFREG | 0700);
        stat.st_size.set(length);
        stat.st_mtim.tv_sec.set(System.currentTimeMillis() / 1000);
        stat.st_mtim.tv_nsec.set(0);
        return OK;
      } else if (isMountPath(path)) {
        stat.st_mode.set(FileStat.S_IFREG | 0700);
        stat.st_size.set(0);
        stat.st_mtim.tv_sec.set(System.currentTimeMillis() / 1000);
        stat.st_mtim.tv_nsec.set(0);
        return OK;
      } else {
        return -ErrorCodes.ENOENT();
      }
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
    try {
      if (isRoot(path)) {
        filter.apply(buf, CURRENT_DIR, null, 0);
        filter.apply(buf, PARENT_DIR, null, 0);
        filter.apply(buf, MOUNT, null, 0);
        filter.apply(buf, STAT, null, 0);
        filter.apply(buf, VOLUME, null, 0);
        return OK;
      } else if (isVolumeRoot(path)) {
        List<String> volumes = getVolumes();
        filter.apply(buf, CURRENT_DIR, null, 0);
        filter.apply(buf, PARENT_DIR, null, 0);
        for (String s : sort(volumes)) {
          filter.apply(buf, s, null, 0);
        }
        return OK;
      } else if (isStatRoot(path)) {
        filter.apply(buf, CURRENT_DIR, null, 0);
        filter.apply(buf, PARENT_DIR, null, 0);
        return OK;
      } else if (isMountRoot(path)) {
        List<String> mountedVolumes = getCurrentMounts();
        filter.apply(buf, CURRENT_DIR, null, 0);
        filter.apply(buf, PARENT_DIR, null, 0);
        for (String s : sort(mountedVolumes)) {
          filter.apply(buf, s, null, 0);
        }
        return OK;
      } else {
        return -ErrorCodes.ENOTDIR();
      }

    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  private List<String> getCurrentMounts() throws TException {
    return getClient().mountedVolumes();
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    try {
      LOGGER.info("open {} {}", path, fi.fh.get());
      if (isRoot(path) || isMountRoot(path) || isVolumeRoot(path) || isStatRoot(path)) {
        return -ErrorCodes.EISDIR();
      } else if (isVolumePath(path)) {
        int handle = getClient().openVolume(getVolumeName(path));
        fi.fh.set(handle);
        return OK;
      } else if (isMountPath(path)) {
        return OK;
      } else if (isStatPath(path)) {
        return OK;
      } else {
        return -ErrorCodes.ENOENT();
      }
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    try {
      LOGGER.info("release {} {}", path, fi.fh.get());
      getClient().releaseVolume(fi.fh.intValue());
      return OK;
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    try {
      int fh = fi.fh.intValue();
      ByteBuffer buffer = getClient().readData(fh, offset);
      int len = buffer.remaining();
      buf.put(0, buffer.array(), 0, len);
      return len;
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    try {
      int fh = fi.fh.intValue();
      int intSize = (int) size;
      byte[] buffer = new byte[intSize];
      buf.get(0, buffer, 0, intSize);
      getClient().writeData(fh, offset, ByteBuffer.wrap(buffer));
      return intSize;
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int mknod(String path, long mode, long rdev) {
    try {
      String volumeName = getVolumeName(path);
      if (getVolumes().contains(volumeName)) {
        return -ErrorCodes.EEXIST();
      }
      getClient().createVolume(volumeName);
      return OK;
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  private List<String> getVolumes() throws TException {
    return getClient().volumes();
  }

  @Override
  public int fallocate(String path, int mode, long off, long length, FuseFileInfo fi) {
    // try {
    // LOGGER.info("fallocate {} {} {} {} {}", path, mode, off, length,
    // fi.fh.get());
    // int fh = fi.fh.intValue();
    // FileHandle handle = getHandle(fh);
    // if (handle == null) {
    // return -ErrorCodes.EIO();
    // }
    // Set<FallocFlags> lookup = FallocFlags.lookup(mode);
    // String volumeName = handle.getVolumeName();
    // if (lookup.contains(FallocFlags.FALLOC_FL_PUNCH_HOLE) &&
    // lookup.contains(FallocFlags.FALLOC_FL_KEEP_SIZE)) {
    // handle.delete(off, length);
    // return OK;
    // } else if (lookup.isEmpty()) {
    // // grow the file
    // long volumeSize = _fileHandleManger.getVolumeSize(volumeName);
    // if (volumeSize == off) {
    // _fileHandleManger.setVolumeSize(volumeName, off + length);
    // return OK;
    // }
    // return -ErrorCodes.EIO();
    // } else {
    // return -ErrorCodes.EIO();
    // }
    // } catch (Throwable t) {
    // LOGGER.error(t.getMessage(), t);
    // return -ErrorCodes.EIO();
    // }
    return OK;
  }

  @Override
  public int access(String path, int mask) {
    return OK;
  }

  @Override
  public int getxattr(String path, String name, Pointer value, long size) {
    return OK;
  }

  @Override
  public int flush(String path, FuseFileInfo fi) {
    return OK;
  }

  @Override
  public int fsync(String path, int isdatasync, FuseFileInfo fi) {
    return OK;
  }

  private boolean isVolumePath(String path) throws Exception {
    String volumeName = getVolumeName(path);
    if (volumeName.contains("/")) {
      return false;
    }
    return getVolumes().contains(volumeName);
  }

  private boolean isRoot(String path) {
    return path.equals("/");
  }

  private String getVolumeName(String path) {
    if (path.startsWith(VOLUME_PREFIX)) {
      return path.substring(VOLUME_PREFIX.length());
    }
    return path;
  }

  private void mkdir(String p) {
    new File(p).mkdirs();
  }

  private static <T extends Comparable<? super T>> Collection<T> sort(Collection<T> col) {
    List<T> list = new ArrayList<>(col);
    Collections.sort(list);
    return list;
  }

  private boolean isMountRoot(String path) {
    return path.equals("/" + MOUNT);
  }

  private boolean isStatRoot(String path) {
    return path.equals("/" + STAT);
  }

  private boolean isVolumeRoot(String path) {
    return path.equals("/" + VOLUME);
  }

  private String getMountName(String path) {
    if (path.startsWith(MOUNT_PREFIX)) {
      return path.substring(MOUNT_PREFIX.length());
    }
    return path;
  }

  private boolean isMountPath(String path) throws TException {
    String volumeName = getMountName(path);
    if (volumeName.contains("/")) {
      return false;
    }
    return getCurrentMounts().contains(volumeName);
  }

  private boolean isStatPath(String path) throws TException {
    String volumeName = getStatName(path);
    if (volumeName.contains("/")) {
      return false;
    }
    return getCurrentMounts().contains(volumeName);
  }

  private String getStatName(String path) {
    if (path.startsWith(STAT_PREFIX)) {
      return path.substring(STAT_PREFIX.length());
    }
    return path;
  }

}
