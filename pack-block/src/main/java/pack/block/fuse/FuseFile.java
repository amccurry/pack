package pack.block.fuse;

import java.io.IOException;

import org.apache.commons.io.IOUtils;

import jnr.ffi.Pointer;
import jnrfuse.struct.FileStat;
import jnrfuse.struct.FuseFileInfo;
import pack.block.blockstore.BlockStore;

public class FuseFile extends FusePath {

  private final long _length;
  private final BlockStore _blockStore;

  public FuseFile(BlockStore blockStore) throws IOException {
    super(blockStore.getName());
    _blockStore = blockStore;
    _length = blockStore.getLength();
  }

  @Override
  protected void getattr(FileStat stat) throws IOException {
    stat.st_mode.set(FileStat.S_IFREG | 0777);
    stat.st_size.set(_length);
    long lastModified = _blockStore.lastModified();
    stat.st_mtim.tv_sec.set(lastModified / 1000);
    stat.st_mtim.tv_nsec.set(0);
  }

  public void fsync(int isdatasync, FuseFileInfo fi) throws IOException {
    _blockStore.fsync();
  }

  public int read(Pointer buffer, long size, long position) throws IOException {
    return readBlockStore(_blockStore, buffer, size, position);
  }

  public int write(Pointer buffer, long size, long position) throws IOException {
    return writeBlockStore(_blockStore, buffer, size, position);
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

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(_blockStore);
  }

  public BlockStore getBlockStore() {
    return _blockStore;
  }
}
