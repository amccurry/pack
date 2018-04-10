package pack.distributed.storage.hdfs.kvs.rpc;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map.Entry;

import org.apache.hadoop.security.UserGroupInformation;

import pack.distributed.storage.hdfs.kvs.BytesRef;
import pack.distributed.storage.hdfs.kvs.KeyValueStore;
import pack.distributed.storage.hdfs.kvs.KeyValueStoreTransId;

public class UgiKeyValueStore implements KeyValueStore {

  public static KeyValueStore wrap(UserGroupInformation ugi, KeyValueStore keyValueStore) {
    return new UgiKeyValueStore(ugi, keyValueStore);
  }

  private final KeyValueStore _delegate;
  private final UserGroupInformation _ugi;

  private UgiKeyValueStore(UserGroupInformation ugi, KeyValueStore delegate) {
    _ugi = ugi;
    _delegate = delegate;
  }

  @Override
  public boolean isOwner() throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> _delegate.isOwner());
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public BytesRef lastKey() throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<BytesRef>) () -> _delegate.lastKey());
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void sync() throws IOException {
    try {
      _ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        _delegate.sync();
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void sync(KeyValueStoreTransId transId) throws IOException {
    try {
      _ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        _delegate.sync(transId);
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterable<Entry<BytesRef, BytesRef>> scan(BytesRef key) throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<Iterable<Entry<BytesRef, BytesRef>>>) () -> _delegate.scan(key));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public KeyValueStoreTransId put(BytesRef key, BytesRef value) throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<KeyValueStoreTransId>) () -> _delegate.put(key, value));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean get(BytesRef key, BytesRef value) throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> _delegate.get(key, value));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public KeyValueStoreTransId delete(BytesRef key) throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<KeyValueStoreTransId>) () -> _delegate.delete(key));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public KeyValueStoreTransId deleteRange(BytesRef fromInclusive, BytesRef toExclusive) throws IOException {
    try {
      return _ugi.doAs(
          (PrivilegedExceptionAction<KeyValueStoreTransId>) () -> _delegate.deleteRange(fromInclusive, toExclusive));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      _ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        _delegate.close();
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

}
