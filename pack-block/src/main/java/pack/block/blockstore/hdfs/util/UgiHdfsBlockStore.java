package pack.block.blockstore.hdfs.util;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;

import pack.block.blockstore.BlockStore;
import pack.block.blockstore.BlockStoreMetaData;
import pack.block.util.Utils;

public class UgiHdfsBlockStore implements BlockStore {

  private final BlockStore _blockStore;

  public UgiHdfsBlockStore(BlockStore blockStore) {
    _blockStore = blockStore;
  }

  public static UgiHdfsBlockStore wrap(BlockStore blockStore) {
    return new UgiHdfsBlockStore(blockStore);
  }

  @Override
  public void close() throws IOException {
    try {
      getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
        _blockStore.close();
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private UserGroupInformation getUgi() throws IOException {
    return Utils.getUserGroupInformation();
  }

  @Override
  public String getName() throws IOException {
    try {
      return getUgi().doAs((PrivilegedExceptionAction<String>) () -> {
        return _blockStore.getName();
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getLength() throws IOException {
    try {
      return getUgi().doAs((PrivilegedExceptionAction<Long>) () -> {
        return _blockStore.getLength();
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long lastModified() throws IOException {
    try {
      return getUgi().doAs((PrivilegedExceptionAction<Long>) () -> {
        return _blockStore.lastModified();
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int write(long position, byte[] buffer, int offset, int len) throws IOException {
    try {
      return getUgi().doAs((PrivilegedExceptionAction<Integer>) () -> {
        return _blockStore.write(position, buffer, offset, len);
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int len) throws IOException {
    try {
      return getUgi().doAs((PrivilegedExceptionAction<Integer>) () -> {
        return _blockStore.read(position, buffer, offset, len);
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void fsync() throws IOException {
    try {
      getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
        _blockStore.fsync();
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public BlockStoreMetaData getMetaData() throws IOException {
    try {
      return getUgi().doAs((PrivilegedExceptionAction<BlockStoreMetaData>) () -> {
        return _blockStore.getMetaData();
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(long position, long length) throws IOException {
    try {
      getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
        _blockStore.delete(position, length);
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getSizeOf() {
    return _blockStore.getSizeOf();
  }
}
