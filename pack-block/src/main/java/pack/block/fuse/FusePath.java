package pack.block.fuse;

import java.io.Closeable;

import jnrfuse.struct.FileStat;

public abstract class FusePath implements Closeable {

  public static final String FILE_SEP = "/";

  protected final String name;

  protected FusePath(String name) {
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
