package pack.distributed.storage.trace;

import java.io.IOException;
import java.util.Map;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.Md5Utils;

import pack.iscsi.storage.utils.PackUtils;

public class TraceStorageModule implements IStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceStorageModule.class);

  private final IStorageModule _delegate;
  private final Map<Long, String> _hashes = new ConcurrentHashMap<>();

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
    int blockSize = getBlockSize();
    int len = bytes.length;
    int off = 0;
    long pos = storageIndex;
    while (len > 0) {
      String current = _hashes.get(pos);
      if (current != null) {
        String bs = Md5Utils.md5AsBase64(PackUtils.copy(bytes, off, blockSize));
        if (!current.equals(bs)) {
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
    int blockSize = getBlockSize();
    int len = bytes.length;
    int off = 0;
    long pos = storageIndex;
    while (len > 0) {
      String md5AsBase64 = Md5Utils.md5AsBase64(PackUtils.copy(bytes, off, blockSize));
      if (pos == 0) {
        LOGGER.info("Write {} {}", pos, md5AsBase64);
      }
      _hashes.put(pos, md5AsBase64);
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

  public static IStorageModule traceIfEnabled(IStorageModule storageModule) {
    if (LOGGER.isTraceEnabled() || Boolean.getBoolean("pack.trace")) {
      return new TraceStorageModule(storageModule);
    }
    return storageModule;
  }
}
