package pack.block.fuse;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.IOUtils;

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
import pack.block.blockstore.BlockStore;

public class FuseFS extends FuseStubFS implements Closeable {

  private static final String FILE_SEP = "/";

  private final String _localPath;

  public FuseFS(String localPath) {
    _localPath = localPath;
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
    synchronized (rootDirectory.contents) {
      for (FusePath path : rootDirectory.contents) {
        if (path instanceof FuseFile) {
          FuseFile file = (FuseFile) path;
          IOUtils.closeQuietly(file._blockStore);
        }
      }
    }
    umount();
  }

  public Collection<String> listBlockStore() {
    Builder<String> builder = ImmutableList.builder();
    synchronized (rootDirectory.contents) {
      for (FusePath path : rootDirectory.contents) {
        if (path instanceof FuseFile) {
          FuseFile file = (FuseFile) path;
          builder.add(file._blockStore.getName());
        }
      }
    }
    return builder.build();
  }

  public synchronized boolean addBlockStore(BlockStore bs) {
    FusePath fusePath = rootDirectory.find(FILE_SEP + bs);
    if (fusePath != null) {
      return false;
    }
    rootDirectory.add(new FuseFile(bs));
    return true;
  }
  
  public BlockStore getBlockStore(String name) {
    FuseFile fuseFile = (FuseFile) rootDirectory.find(FILE_SEP + name);
    return fuseFile._blockStore;
  }

  public BlockStore removeBlockStore(String name) {
    FuseFile fuseFile = (FuseFile) rootDirectory.find(FILE_SEP + name);
    rootDirectory.deleteChild(fuseFile);
    return fuseFile._blockStore;
  }

  private class FuseDirectory extends FusePath {

    private List<FusePath> contents = new ArrayList<>();

    private FuseDirectory(String name) {
      super(name);
    }

    public synchronized void add(FusePath p) {
      contents.add(p);
    }

    private synchronized void deleteChild(FusePath child) {
      contents.remove(child);
    }

    @Override
    protected FusePath find(String path) {
      if (super.find(path) != null) {
        return super.find(path);
      }
      while (path.startsWith(FILE_SEP)) {
        path = path.substring(1);
      }
      synchronized (this) {
        if (!path.contains(FILE_SEP)) {
          for (FusePath p : contents) {
            if (p.name.equals(path)) {
              return p;
            }
          }
          return null;
        }
        String nextName = path.substring(0, path.indexOf(FILE_SEP));
        String rest = path.substring(path.indexOf(FILE_SEP));
        for (FusePath p : contents) {
          if (p.name.equals(nextName)) {
            return p.find(rest);
          }
        }
      }
      return null;
    }

    @Override
    protected void getattr(FileStat stat) {
      stat.st_mode.set(FileStat.S_IFDIR | 0777);
    }

    public synchronized void read(Pointer buf, FuseFillDir filler) {
      for (FusePath p : contents) {
        filler.apply(buf, p.name, null, 0);
      }
    }
  }

  private class FuseFile extends FusePath {

    private final long _length;
    private final BlockStore _blockStore;

    public FuseFile(BlockStore blockStore) {
      super(blockStore.getName());
      _blockStore = blockStore;
      _length = blockStore.getLength();
    }

    @Override
    protected void getattr(FileStat stat) {
      stat.st_mode.set(FileStat.S_IFREG | 0777);
      stat.st_size.set(_length);
      long lastModified = _blockStore.lastModified();
      stat.st_mtim.tv_sec.set(lastModified / 1000);
      stat.st_mtim.tv_nsec.set(0);
    }

    private int read(Pointer buffer, long size, long position) throws IOException {
      int len = (int) size;
      int offset = 0;
      while (len > 0) {
        int write = _blockStore.read(position, buffer, offset, len);
        len -= write;
        offset += write;
        position += write;
      }
      return offset;
    }

    private int write(Pointer buffer, long size, long position) throws IOException {
      int len = (int) size;
      int offset = 0;
      while (len > 0) {
        int write = _blockStore.write(position, buffer, offset, len);
        len -= write;
        offset += write;
        position += write;
      }
      return offset;
    }

    private void fsync(int isdatasync, FuseFileInfo fi) throws IOException {
      _blockStore.fsync();
    }
  }

  private abstract class FusePath {
    private String name;

    private FusePath(String name) {
      this.name = name;
    }

    protected FusePath find(String path) {
      while (path.startsWith("/")) {
        path = path.substring(1);
      }
      if (path.equals(name) || path.isEmpty()) {
        return this;
      }
      return null;
    }

    protected abstract void getattr(FileStat stat);

  }

  private FuseDirectory rootDirectory = new FuseDirectory("");

  @Override
  public int getattr(String path, FileStat stat) {
    FusePath p = getPath(path);
    if (p != null) {
      p.getattr(stat);
      return 0;
    }
    return -ErrorCodes.ENOENT();
  }

  private FusePath getPath(String path) {
    return rootDirectory.find(path);
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
