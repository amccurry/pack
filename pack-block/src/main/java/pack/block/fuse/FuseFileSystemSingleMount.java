package pack.block.fuse;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnrfuse.ErrorCodes;
import jnrfuse.FuseFillDir;
import jnrfuse.FuseStubFS;
import jnrfuse.struct.FileStat;
import jnrfuse.struct.FuseFileInfo;
import pack.block.blockstore.BlockStore;

public class FuseFileSystemSingleMount extends FuseStubFS implements Closeable {

  public static final String FUSE_PID = "fuse_pid";
  private static final String PARENT_DIR = "..";
  private static final String CURRENT_DIR = ".";
  private static final String FILE_SEP = "/";
  private static final String PID_FILENAME = FILE_SEP + FUSE_PID;
  private static final String OPTION_SWITCH = "-o";
  private static final String AUTO_UNMOUNT = "auto_unmount";
  private static final String ALLOW_ROOT = "allow_root";
  private final Logger _logger;
  private final String _localPath;
  private final BlockStore _blockStore;
  private final String _mountPath;
  private final long _length;
  private final byte[] _pidContent;

  public FuseFileSystemSingleMount(String localPath, BlockStore blockStore) {
    _logger = LoggerFactory.getLogger(FuseFileSystemSingleMount.class);
    _localPath = localPath;
    _blockStore = blockStore;
    _length = blockStore.getLength();
    _mountPath = FILE_SEP + blockStore.getName();
    _pidContent = ManagementFactory.getRuntimeMXBean()
                                   .getName()
                                   .getBytes();
  }

  public void localMount() {
    String[] opts = new String[] { OPTION_SWITCH, ALLOW_ROOT, OPTION_SWITCH, AUTO_UNMOUNT };
    mount(Paths.get(_localPath), true, false, opts);
  }

  public void localMount(boolean blocking) {
    String[] opts = new String[] { OPTION_SWITCH, ALLOW_ROOT, OPTION_SWITCH, AUTO_UNMOUNT };
    mount(Paths.get(_localPath), blocking, false, opts);
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(_blockStore);
    umount();
  }

  @Override
  public int getattr(String path, FileStat stat) {
    long lastModified = _blockStore.lastModified();
    stat.st_mtim.tv_sec.set(lastModified / 1000);
    stat.st_mtim.tv_nsec.set(0);
    if (path.equals(FILE_SEP)) {
      stat.st_mode.set(FileStat.S_IFDIR | 0777);
      stat.st_size.set(2);
      return 0;
    } else if (path.equals(_mountPath)) {
      stat.st_mode.set(FileStat.S_IFREG | 0777);
      stat.st_size.set(_length);
      return 0;
    } else if (path.equals(PID_FILENAME)) {
      stat.st_mode.set(FileStat.S_IFREG | 0444);
      stat.st_size.set(_pidContent.length);
      return 0;
    } else {
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
    if (path.equals(FILE_SEP)) {
      filter.apply(buf, CURRENT_DIR, null, 0);
      filter.apply(buf, PARENT_DIR, null, 0);
      filter.apply(buf, _blockStore.getName(), null, 0);
      filter.apply(buf, FUSE_PID, null, 0);
      return 0;
    } else if (path.equals(_mountPath)) {
      return -ErrorCodes.ENOTDIR();
    } else {
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    if (path.equals(FILE_SEP)) {
      return -ErrorCodes.EISDIR();
    } else if (path.equals(_mountPath)) {
      try {
        _logger.debug("read {} position {} length {}", path, offset, size);
        return readBlockStore(_blockStore, buf, size, offset);
      } catch (Throwable t) {
        _logger.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    } else if (path.equals(PID_FILENAME)) {
      buf.put(0, _pidContent, 0, _pidContent.length);
      return _pidContent.length;
    } else {
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    if (path.equals(FILE_SEP)) {
      return -ErrorCodes.EISDIR();
    } else if (path.equals(_mountPath)) {
      try {
        _logger.debug("write {} position {} length {}", path, offset, size);
        return writeBlockStore(_blockStore, buf, size, offset);
      } catch (Throwable t) {
        _logger.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    } else if (path.equals(PID_FILENAME)) {
      return -ErrorCodes.EIO();
    } else {
      return -ErrorCodes.ENOENT();
    }
  }

  @Override
  public int fsync(String path, int isdatasync, FuseFileInfo fi) {
    if (path.equals(FILE_SEP)) {
      return -ErrorCodes.EISDIR();
    } else if (path.equals(_mountPath)) {
      try {
        _blockStore.fsync();
        return 0;
      } catch (Throwable t) {
        _logger.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    } else {
      return -ErrorCodes.ENOENT();
    }
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
