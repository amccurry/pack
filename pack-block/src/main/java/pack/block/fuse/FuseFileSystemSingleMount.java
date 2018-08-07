package pack.block.fuse;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnrfuse.ErrorCodes;
import jnrfuse.FuseFillDir;
import jnrfuse.FuseStubFS;
import jnrfuse.flags.FallocFlags;
import jnrfuse.struct.FileStat;
import jnrfuse.struct.FuseFileInfo;
import pack.block.blockstore.BlockStore;

public class FuseFileSystemSingleMount extends FuseStubFS implements Closeable {

  private static final String NONEMPTY = "nonempty";

  private static final Logger LOGGER = LoggerFactory.getLogger(FuseFileSystemSingleMount.class);

  public static final String BRICK = "brick";
  public static final String FUSE_PID = "fuse_pid";
  private static final String SYNC = "sync";
  private static final String PARENT_DIR = "..";
  private static final String CURRENT_DIR = ".";
  private static final String FILE_SEP = "/";
  private static final String PID_FILENAME = FILE_SEP + FUSE_PID;
  private static final String BRICK_FILENAME = FILE_SEP + BRICK;
  private static final String SHUTDOWN = "shutdown";
  private static final String SHUTDOWN_FILENAME = FILE_SEP + SHUTDOWN;
  private static final String SNAPSHOT = "snapshot";
  private static final String SNAPSHOT_FILENAME = FILE_SEP + SNAPSHOT;
  private static final String OPTION_SWITCH = "-o";
  private static final String AUTO_UNMOUNT = "auto_unmount";
  private static final String ALLOW_ROOT = "allow_root";
  private final String _localPath;
  private final BlockStore _blockStore;
  private final byte[] _pidContent;
  private final SnapshotHandler _snapshotHandler;

  public FuseFileSystemSingleMount(String localPath, BlockStore blockStore, SnapshotHandler snapshotHandler)
      throws IOException {
    _snapshotHandler = snapshotHandler;
    _localPath = localPath;
    _blockStore = blockStore;
    _pidContent = (ManagementFactory.getRuntimeMXBean()
                                    .getName()
        + "\n").getBytes();
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
    mount(Paths.get(_localPath), blocking, false, opts);
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("close");
    IOUtils.closeQuietly(_blockStore);
    umount();
  }

  @Override
  public int truncate(String path, @off_t long size) {
    switch (path) {
    case FILE_SEP:
      return -ErrorCodes.EISDIR();
    case PID_FILENAME:
    case BRICK_FILENAME:
      return -ErrorCodes.EIO();
    case SNAPSHOT_FILENAME:
    case SHUTDOWN_FILENAME:
      return 0;
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int getattr(String path, FileStat stat) {
    long lastModified;
    long length;
    try {
      lastModified = _blockStore.lastModified();
      length = _blockStore.getLength();
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
    stat.st_mtim.tv_sec.set(lastModified / 1000);
    stat.st_mtim.tv_nsec.set(0);
    switch (path) {
    case BRICK_FILENAME:
      stat.st_mode.set(FileStat.S_IFREG | 0700);
      stat.st_size.set(length);
      return 0;
    case FILE_SEP:
      stat.st_mode.set(FileStat.S_IFDIR | 0755);
      stat.st_size.set(2);
      return 0;
    case PID_FILENAME:
      stat.st_mode.set(FileStat.S_IFREG | 0400);
      stat.st_size.set(_pidContent.length);
      return 0;
    case SNAPSHOT_FILENAME:
    case SHUTDOWN_FILENAME:
      stat.st_mode.set(FileStat.S_IFREG | 0600);
      stat.st_size.set(0);
      return 0;
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
    switch (path) {
    case FILE_SEP:
      filter.apply(buf, CURRENT_DIR, null, 0);
      filter.apply(buf, PARENT_DIR, null, 0);
      filter.apply(buf, BRICK, null, 0);
      filter.apply(buf, FUSE_PID, null, 0);
      filter.apply(buf, SHUTDOWN, null, 0);
      filter.apply(buf, SNAPSHOT, null, 0);
      return 0;
    case BRICK_FILENAME:
    case PID_FILENAME:
    case SNAPSHOT_FILENAME:
    case SHUTDOWN_FILENAME:
      return -ErrorCodes.ENOTDIR();
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    switch (path) {
    case BRICK_FILENAME:
      while (true) {
        try {
          LOGGER.debug("read {} position {} length {}", path, offset, size);
          return readBlockStore(_blockStore, buf, size, offset);
        } catch (Throwable t) {
          LOGGER.error("Unknown error.", t);
          sleep(TimeUnit.SECONDS.toMillis(3));
        }
      }
    case FILE_SEP:
      return -ErrorCodes.EISDIR();
    case PID_FILENAME:
      buf.put(0, _pidContent, 0, _pidContent.length);
      return _pidContent.length;
    case SNAPSHOT_FILENAME:
    case SHUTDOWN_FILENAME:
      buf.put(0, new byte[] {}, 0, 0);
      return 0;
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(3));
    } catch (InterruptedException e) {
      LOGGER.error("Unknown error", e);
    }
  }

  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    switch (path) {
    case BRICK_FILENAME:
      while (true) {
        try {
          LOGGER.debug("write {} position {} length {}", path, offset, size);
          return writeBlockStore(_blockStore, buf, size, offset);
        } catch (Throwable t) {
          LOGGER.error("Unknown error.", t);
          sleep(TimeUnit.SECONDS.toMillis(3));
        }
      }
    case SNAPSHOT_FILENAME: {
      try {
        return createNewSnapshot(buf, size, offset);
      } catch (Throwable t) {
        LOGGER.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    }
    case SHUTDOWN_FILENAME: {
      startShutdown();
      return (int) size;
    }
    case FILE_SEP:
      return -ErrorCodes.EISDIR();
    case PID_FILENAME:
      return -ErrorCodes.EIO();
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  private void startShutdown() {
    new Thread(() -> System.exit(0)).start();
  }

  @Override
  public int fsync(String path, int isdatasync, FuseFileInfo fi) {
    LOGGER.debug("fsync {} {} {}", path, isdatasync, fi);
    switch (path) {
    case BRICK_FILENAME:
      while (true) {
        try {
          _blockStore.fsync();
          return 0;
        } catch (Throwable t) {
          LOGGER.error("Unknown error.", t);
          sleep(TimeUnit.SECONDS.toMillis(3));
        }
      }
    case FILE_SEP:
      return -ErrorCodes.EISDIR();
    case SNAPSHOT_FILENAME:
    case SHUTDOWN_FILENAME:
    case PID_FILENAME:
      return 0;
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int fallocate(String path, int mode, long off, long length, FuseFileInfo fi) {
    LOGGER.debug("fallocate {} {} {} {} {}", path, mode, off, length, fi);
    Set<FallocFlags> lookup = FallocFlags.lookup(mode);
    switch (path) {
    case BRICK_FILENAME:
      if (lookup.contains(FallocFlags.FALLOC_FL_PUNCH_HOLE) && lookup.contains(FallocFlags.FALLOC_FL_KEEP_SIZE)) {
        while (true) {
          try {
            _blockStore.delete(off, length);
            return 0;
          } catch (Throwable t) {
            LOGGER.error("Unknown error.", t);
            sleep(TimeUnit.SECONDS.toMillis(3));
          }
        }
      } else {
        return -ErrorCodes.EOPNOTSUPP();
      }
    case FILE_SEP:
      return -ErrorCodes.EISDIR();
    case SNAPSHOT_FILENAME:
    case SHUTDOWN_FILENAME:
    case PID_FILENAME:
      return -ErrorCodes.EIO();
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    LOGGER.debug("open {} {}", path, fi);
    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    LOGGER.debug("release {} {}", path, fi);
    return fsync(path, 0, fi);
  }

  @Override
  public int flush(String path, FuseFileInfo fi) {
    LOGGER.debug("flush {} {}", path, fi);
    return fsync(path, 0, fi);
  }

  public static int readBlockStore(BlockStore blockStore, Pointer buffer, long size, long position) throws IOException {
    int len = (int) size;
    byte[] buf = new byte[len];
    int offset = 0;
    while (len > 0) {
      int read = blockStore.read(position, buf, offset, len);
      len -= read;
      offset += read;
      position += read;
    }
    buffer.put(0, buf, 0, offset);
    return offset;
  }

  public static int writeBlockStore(BlockStore blockStore, Pointer buffer, long size, long position)
      throws IOException {
    int len = (int) size;

    byte[] buf = new byte[len];
    buffer.get(0, buf, 0, len);

    int offset = 0;
    while (len > 0) {
      int write = blockStore.write(position, buf, offset, len);
      len -= write;
      offset += write;
      position += write;
    }
    return offset;
  }

  private int createNewSnapshot(Pointer buffer, long size, long position) throws Exception {
    int len = (int) size;

    byte[] buf = new byte[len];
    buffer.get(0, buf, 0, len);

    String s = new String(buf, 0, len);
    String name = s.trim();
    LOGGER.info("Creating new volume snapshot {}", name);
    _snapshotHandler.createNewSnapshot(name);
    return len;
  }

}
