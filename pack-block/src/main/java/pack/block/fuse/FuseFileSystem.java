package pack.block.fuse;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import jnr.ffi.Pointer;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnrfuse.ErrorCodes;
import jnrfuse.FuseFillDir;
import jnrfuse.FuseStubFS;
import jnrfuse.struct.FileStat;
import jnrfuse.struct.FuseFileInfo;
import pack.block.blockstore.BlockStore;

public class FuseFileSystem extends FuseStubFS implements Closeable {

  private final Logger _logger;
  private final String _localPath;

  public FuseFileSystem(String localPath) {
    _localPath = localPath;
    _logger = LoggerFactory.getLogger(FuseFileSystem.class);
  }

  public void localMount() {
    String[] opts = new String[] { "-o", "allow_root", "-o", "auto_unmount" };
    mount(Paths.get(_localPath), true, false, opts);
  }

  public void localMount(boolean blocking) {
    String[] opts = new String[] { "-o", "allow_root", "-o", "auto_unmount" };
    mount(Paths.get(_localPath), blocking, false, opts);
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(rootDirectory);
    umount();
  }

  public Collection<String> listBlockStore() {
    return ImmutableList.copyOf(rootDirectory.getBlockStoreNames());
  }

  public synchronized boolean addBlockStore(BlockStore bs) {
    FusePath fusePath = rootDirectory.find(FusePath.FILE_SEP + bs);
    if (fusePath != null) {
      return false;
    }
    rootDirectory.add(new FuseFile(bs));
    return true;
  }

  public BlockStore getBlockStore(String name) {
    FuseFile fuseFile = (FuseFile) rootDirectory.find(FusePath.FILE_SEP + name);
    return fuseFile.getBlockStore();
  }

  public BlockStore removeBlockStore(String name) {
    FuseFile fuseFile = (FuseFile) rootDirectory.find(FusePath.FILE_SEP + name);
    rootDirectory.deleteChild(fuseFile);
    return fuseFile.getBlockStore();
  }

  private FuseDirectory rootDirectory = new FuseDirectory("");

  private FusePath getPath(String path) {
    return rootDirectory.find(path);
  }

  @Override
  public int getattr(String path, FileStat stat) {
    FusePath p = getPath(path);
    if (p != null) {
      p.getattr(stat);
      return 0;
    }
    return -ErrorCodes.ENOENT();
  }

  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    FusePath p = getPath(path);
    if (p == null) {
      return -ErrorCodes.ENOENT();
    }
    if (!(p instanceof FuseFile)) {
      return -ErrorCodes.EISDIR();
    }
    try {
      // _logger.info("read {} position {} length {}", path, offset, size);
      return ((FuseFile) p).read(buf, size, offset);
    } catch (IOException e) {
      e.printStackTrace();
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
    FusePath p = getPath(path);
    if (p == null) {
      return -ErrorCodes.ENOENT();
    }
    if (!(p instanceof FuseDirectory)) {
      return -ErrorCodes.ENOTDIR();
    }
    filter.apply(buf, ".", null, 0);
    filter.apply(buf, "..", null, 0);
    ((FuseDirectory) p).read(buf, filter);
    return 0;
  }

  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    FusePath p = getPath(path);
    if (p == null) {
      return -ErrorCodes.ENOENT();
    }
    if (!(p instanceof FuseFile)) {
      return -ErrorCodes.EISDIR();
    }
    try {
      // _logger.info("write {} position {} length {}", path, offset, size);
      return ((FuseFile) p).write(buf, size, offset);
    } catch (IOException e) {
      e.printStackTrace();
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int fsync(String path, int isdatasync, FuseFileInfo fi) {
    FusePath p = getPath(path);
    if (p == null) {
      return -ErrorCodes.ENOENT();
    }
    if (!(p instanceof FuseFile)) {
      return -ErrorCodes.EISDIR();
    }
    try {
      ((FuseFile) p).fsync(isdatasync, fi);
      return 0;
    } catch (IOException e) {
      e.printStackTrace();
      return -ErrorCodes.EIO();
    }
  }

}
