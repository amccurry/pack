package pack.block.blockstore.hdfs;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;

import pack.block.server.fs.LinuxFileSystem;

public class UgiHdfsBlockStore implements HdfsBlockStore {

  private final UserGroupInformation _ugi;
  private final HdfsBlockStore _blockStore;

  public UgiHdfsBlockStore(UserGroupInformation ugi, HdfsBlockStore blockStore) {
    _ugi = ugi;
    _blockStore = blockStore;
  }

  public static UgiHdfsBlockStore wrap(UserGroupInformation ugi, HdfsBlockStore blockStore) {
    return new UgiHdfsBlockStore(ugi, blockStore);
  }

  @Override
  public void close() throws IOException {
    try {
      _ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        _blockStore.close();
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public LinuxFileSystem getLinuxFileSystem() throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<LinuxFileSystem>) () -> {
        return _blockStore.getLinuxFileSystem();
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public String getName() throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<String>) () -> {
        return _blockStore.getName();
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getLength() throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<Long>) () -> {
        return _blockStore.getLength();
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long lastModified() throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<Long>) () -> {
        return _blockStore.lastModified();
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int write(long position, byte[] buffer, int offset, int len) throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<Integer>) () -> {
        return _blockStore.write(position, buffer, offset, len);
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int len) throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<Integer>) () -> {
        return _blockStore.read(position, buffer, offset, len);
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void fsync() throws IOException {
    try {
      _ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        _blockStore.fsync();
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public HdfsMetaData getMetaData() throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<HdfsMetaData>) () -> {
        return _blockStore.getMetaData();
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(long position, long length) throws IOException {
    try {
      _ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        _blockStore.delete(position, length);
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

}
