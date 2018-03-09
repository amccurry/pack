package pack.distributed.storage;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceStorageModule implements IStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceStorageModule.class);

  private final IStorageModule _delegate;
  private final Map<Long, byte[]> _hashes = new ConcurrentHashMap<>();

  public TraceStorageModule(IStorageModule delegate) {
    _delegate = delegate;
  }

  @Override
  public int checkBounds(long logicalBlockAddress, int transferLengthInBlocks) {
    return _delegate.checkBounds(logicalBlockAddress, transferLengthInBlocks);
  }

  @Override
  public long getSizeInBlocks() {
    return _delegate.getSizeInBlocks();
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    _delegate.read(bytes, storageIndex);
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("md5");
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
    int blockSize = getBlockSize();
    int len = bytes.length;
    int off = 0;
    long pos = storageIndex;
    while (len > 0) {
      byte[] current = _hashes.get(pos);
      if (current != null) {
        byte[] bs = digest.digest(copy(bytes, off, blockSize));
        if (!Arrays.equals(current, bs)) {
          LOGGER.error("Pos {} not equal {} {}", pos, current, bs);
        }
      }
      len -= blockSize;
      off += blockSize;
      pos += blockSize;
    }
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("md5");
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
    int blockSize = getBlockSize();
    int len = bytes.length;
    int off = 0;
    long pos = storageIndex;
    while (len > 0) {
      byte[] bs = digest.digest(copy(bytes, off, blockSize));
      _hashes.put(pos, bs);
      len -= blockSize;
      off += blockSize;
      pos += blockSize;
    }
    _delegate.write(bytes, storageIndex);
  }

  @Override
  public void close() throws IOException {
    _delegate.close();
  }

  @Override
  public int getBlockSize() {
    return _delegate.getBlockSize();
  }

  private byte[] copy(byte[] bytes, int off, int len) {
    byte[] buf = new byte[len];
    System.arraycopy(bytes, off, buf, 0, len);
    return buf;
  }
}
