package pack.block.fuse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import jnr.ffi.Pointer;
import jnrfuse.FuseFillDir;
import jnrfuse.struct.FileStat;

public class FuseDirectory extends FusePath {

  private List<FusePath> contents = new ArrayList<>();

  public FuseDirectory(String name) {
    super(name);
  }

  public void add(FusePath p) {
    synchronized (contents) {
      contents.add(p);
    }
  }

  public void deleteChild(FusePath child) {
    synchronized (contents) {
      contents.remove(child);
    }
  }

  public List<FusePath> list() {
    synchronized (contents) {
      return ImmutableList.copyOf(contents);
    }
  }

  @Override
  protected FusePath find(String path) {
    if (super.find(path) != null) {
      return super.find(path);
    }
    while (path.startsWith(FILE_SEP)) {
      path = path.substring(1);
    }
    synchronized (contents) {
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

  public void read(Pointer buf, FuseFillDir filler) {
    synchronized (contents) {
      for (FusePath p : contents) {
        filler.apply(buf, p.name, null, 0);
      }
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (contents) {
      for (FusePath fusePath : contents) {
        IOUtils.closeQuietly(fusePath);
      }
    }
  }

  public List<String> getBlockStoreNames() throws IOException {
    Builder<String> builder = ImmutableList.builder();
    synchronized (contents) {
      for (FusePath p : contents) {
        if (p instanceof FuseFile) {
          FuseFile file = (FuseFile) p;
          builder.add(file.getBlockStore()
                          .getName());
        }
      }
    }
    return builder.build();
  }
}
