package pack.s3;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.commons.io.IOUtils;
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
import jnrfuse.flags.FallocFlags;
import jnrfuse.struct.FileStat;
import jnrfuse.struct.FuseFileInfo;

public class PackV2 extends FuseStubFS implements Closeable {

  private static final String BIG_WRITES = "big_writes";

  private static final Logger LOGGER = LoggerFactory.getLogger(PackV2.class);

  private static final String NONEMPTY = "nonempty";
  private static final String SYNC = "sync";
  private static final String PARENT_DIR = "..";
  private static final String CURRENT_DIR = ".";
  private static final String OPTION_SWITCH = "-o";
  private static final String AUTO_UNMOUNT = "auto_unmount";
  private static final String ALLOW_ROOT = "allow_root";
  private static final int OK = 0;

  private final String _localPath;
  private final AtomicReferenceArray<FileHandle> _handles = new AtomicReferenceArray<>(4 * 1024);

  private final boolean _sync = true;
  private final boolean _bigWrites = true;
  private final FileHandleManger _fileHandleManger;

  public static void main(String[] args) throws Exception {
    try (PackV2 pack = new PackV2(args[0], args[1], args[2], args[3], args[4])) {
      pack.localMount();
    }
  }

  public PackV2(String localPath, String cache, String sync, String zk, String bucketName) throws Exception {
    mkdir(localPath);
    mkdir(cache);
    _localPath = localPath;
    _fileHandleManger = new FileHandleManger(cache, sync, zk, bucketName);
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

  @Override
  public int getattr(String path, FileStat stat) {
    try {
      List<String> volumes = _fileHandleManger.getVolumes();
      if (isRoot(path)) {
        stat.st_mode.set(FileStat.S_IFDIR | 0755);
        stat.st_size.set(volumes.size());
        stat.st_mtim.tv_sec.set(System.currentTimeMillis() / 1000);
        stat.st_mtim.tv_nsec.set(0);
        return OK;
      } else if (isVolume(path)) {
        String volumeName = getVolumeName(path);
        long length = _fileHandleManger.getVolumeSize(volumeName);
        stat.st_mode.set(FileStat.S_IFREG | 0700);
        stat.st_size.set(length);
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
      if (!isRoot(path)) {
        return -ErrorCodes.ENOTDIR();
      }

      List<String> volumes = _fileHandleManger.getVolumes();
      filter.apply(buf, CURRENT_DIR, null, 0);
      filter.apply(buf, PARENT_DIR, null, 0);
      for (String s : volumes) {
        filter.apply(buf, s, null, 0);
      }
      return OK;
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    try {
      LOGGER.info("open {} {}", path, fi.fh.get());
      if (isRoot(path)) {
        return -ErrorCodes.EISDIR();
      }

      if (!isVolume(path)) {
        return -ErrorCodes.ENOENT();
      }

      FileHandle fileHandle = _fileHandleManger.getHandle(getVolumeName(path));
      int handle = registerHandle(fileHandle);
      fi.fh.set(handle);
      return OK;
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  private int registerHandle(FileHandle fileHandle) throws IOException {
    synchronized (_handles) {
      for (int i = 0; i < _handles.length(); i++) {
        FileHandle h = _handles.get(i);
        if (h == null) {
          _handles.set(i, fileHandle);
          return i;
        }
      }
      throw new IOException("Too many open handles.");
    }
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    try {
      LOGGER.info("release {} {}", path, fi.fh.get());
      closeHandle(fi.fh.intValue());
      return OK;
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  // @Override
  // public int mknod(String path, long mode, long rdev) {
  // try {
  // if (!FileStat.S_ISREG((int) mode)) {
  // return -ErrorCodes.EIO();
  // }
  // File file = getFile(path);
  // touch(file);
  // return OK;
  // } catch (Throwable t) {
  // LOGGER.error(t.getMessage(), t);
  // return -ErrorCodes.EIO();
  // }
  // }

  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    try {
      int fh = fi.fh.intValue();
      FileHandle handle = getHandle(fh);
      if (handle == null) {
        return -ErrorCodes.EIO();
      }
      return handle.read(buf, (int) size, offset);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    try {
      int fh = fi.fh.intValue();
      FileHandle handle = getHandle(fh);
      if (handle == null) {
        return -ErrorCodes.EIO();
      }
      return handle.write(buf, (int) size, offset);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int fallocate(String path, int mode, long off, long length, FuseFileInfo fi) {
    try {
      LOGGER.info("fallocate {} {} {} {} {}", path, mode, off, length, fi.fh.get());
      int fh = fi.fh.intValue();
      FileHandle handle = getHandle(fh);
      if (handle == null) {
        return -ErrorCodes.EIO();
      }
      Set<FallocFlags> lookup = FallocFlags.lookup(mode);
      String volumeName = handle.getVolumeName();
      if (lookup.contains(FallocFlags.FALLOC_FL_PUNCH_HOLE) && lookup.contains(FallocFlags.FALLOC_FL_KEEP_SIZE)) {
        handle.delete(off, length);
        return OK;
      } else if (lookup.isEmpty()) {
        // grow the file
        long volumeSize = _fileHandleManger.getVolumeSize(volumeName);
        if (volumeSize == off) {
          _fileHandleManger.setVolumeSize(volumeName, off + length);
          return OK;
        }
        return -ErrorCodes.EIO();
      } else {
        return -ErrorCodes.EIO();
      }
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return -ErrorCodes.EIO();
    }
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

  private void closeHandle(int handle) throws IOException {
    synchronized (_handles) {
      IOUtils.closeQuietly(_handles.getAndSet(handle, null));
    }
  }

  private boolean isVolume(String path) throws Exception {
    String volumeName = getVolumeName(path);
    if (volumeName.contains("/")) {
      return false;
    }
    return _fileHandleManger.getVolumes()
                            .contains(volumeName);
  }

  private boolean isRoot(String path) {
    return path.equals("/");
  }

  private String getVolumeName(String path) {
    if (path.startsWith("/")) {
      return path.substring(1);
    }
    return path;
  }

  private FileHandle getHandle(int fh) {
    return _handles.get(fh);
  }

  private void mkdir(String p) {
    new File(p).mkdirs();
  }
}
