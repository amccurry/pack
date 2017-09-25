package pack.block.fuse;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Paths;
import java.util.Set;

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

  public static final String BRICK = "brick";
  public static final String FUSE_PID = "fuse_pid";
  private static final String PARENT_DIR = "..";
  private static final String CURRENT_DIR = ".";
  private static final String FILE_SEP = "/";
  private static final String PID_FILENAME = FILE_SEP + FUSE_PID;
  private static final String BRICK_FILENAME = FILE_SEP + BRICK;
  private static final String OPTION_SWITCH = "-o";
  private static final String AUTO_UNMOUNT = "auto_unmount";
  private static final String ALLOW_ROOT = "allow_root";
  private final Logger _logger;
  private final String _localPath;
  private final BlockStore _blockStore;
  private final long _length;
  private final byte[] _pidContent;

  public FuseFileSystemSingleMount(String localPath, BlockStore blockStore) throws IOException {
    _logger = LoggerFactory.getLogger(FuseFileSystemSingleMount.class);
    _localPath = localPath;
    _blockStore = blockStore;
    _length = blockStore.getLength();
    _pidContent = ManagementFactory.getRuntimeMXBean()
                                   .getName()
                                   .getBytes();
  }

  public void localMount() {
    localMount(true);
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
      opts = new String[] { OPTION_SWITCH, ALLOW_ROOT, OPTION_SWITCH, AUTO_UNMOUNT };
      break;
    }
    mount(Paths.get(_localPath), blocking, false, opts);
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(_blockStore);
    umount();
  }

  @Override
  public int getattr(String path, FileStat stat) {
    long lastModified;
    try {
      lastModified = _blockStore.lastModified();
    } catch (Throwable t) {
      _logger.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
    stat.st_mtim.tv_sec.set(lastModified / 1000);
    stat.st_mtim.tv_nsec.set(0);
    switch (path) {
    case BRICK_FILENAME:
      stat.st_mode.set(FileStat.S_IFREG | 0777);
      stat.st_size.set(_length);
      return 0;
    case FILE_SEP:
      stat.st_mode.set(FileStat.S_IFDIR | 0777);
      stat.st_size.set(2);
      return 0;
    case PID_FILENAME:
      stat.st_mode.set(FileStat.S_IFREG | 0444);
      stat.st_size.set(_pidContent.length);
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
      return 0;
    case BRICK_FILENAME:
    case PID_FILENAME:
      return -ErrorCodes.ENOTDIR();
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    switch (path) {
    case BRICK_FILENAME:
      try {
        _logger.info("read {} position {} length {}", path, offset, size);
        return readBlockStore(_blockStore, buf, size, offset);
      } catch (Throwable t) {
        _logger.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    case FILE_SEP:
      return -ErrorCodes.EISDIR();
    case PID_FILENAME:
      buf.put(0, _pidContent, 0, _pidContent.length);
      return _pidContent.length;
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    switch (path) {
    case BRICK_FILENAME:
      try {
        _logger.info("write {} position {} length {}", path, offset, size);
        return writeBlockStore(_blockStore, buf, size, offset);
      } catch (Throwable t) {
        _logger.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    case FILE_SEP:
      return -ErrorCodes.EISDIR();
    case PID_FILENAME:
      return -ErrorCodes.EIO();
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int fsync(String path, int isdatasync, FuseFileInfo fi) {
    _logger.info("fsync {} {} {}", path, isdatasync, fi);
    switch (path) {
    case BRICK_FILENAME:
      try {
        _blockStore.fsync();
        return 0;
      } catch (Throwable t) {
        _logger.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    case FILE_SEP:
      return -ErrorCodes.EISDIR();
    case PID_FILENAME:
      return -ErrorCodes.EIO();
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int fallocate(String path, int mode, long off, long length, FuseFileInfo fi) {
    _logger.info("fallocate {} {} {} {} {}", path, mode, off, length, fi);
    Set<FallocFlags> lookup = FallocFlags.lookup(mode);
    switch (path) {
    case BRICK_FILENAME:
      if (lookup.contains(FallocFlags.FALLOC_FL_PUNCH_HOLE) && lookup.contains(FallocFlags.FALLOC_FL_KEEP_SIZE)) {
        try {
          _blockStore.delete(off, length);
          return 0;
        } catch (Throwable t) {
          _logger.error("Unknown error.", t);
          return -ErrorCodes.EIO();
        }
      } else {
        return -ErrorCodes.EOPNOTSUPP();
      }
    case FILE_SEP:
      return -ErrorCodes.EISDIR();
    case PID_FILENAME:
      return -ErrorCodes.EIO();
    default:
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    _logger.info("open {} {}", path, fi);
    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    _logger.info("release {} {}", path, fi);
    return 0;
  }

  @Override
  public int flush(String path, FuseFileInfo fi) {
    _logger.info("flush {} {}", path, fi);
    return 0;
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

}
