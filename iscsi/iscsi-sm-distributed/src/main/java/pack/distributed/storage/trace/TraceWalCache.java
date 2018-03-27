package pack.distributed.storage.trace;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.Md5Utils;

import pack.distributed.storage.hdfs.BlockFile.Writer;
import pack.distributed.storage.read.BlockReader;
import pack.distributed.storage.read.ReadRequest;
import pack.distributed.storage.wal.WalCache;

public class TraceWalCache implements WalCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceWalCache.class);
  private final WalCache _delegate;
  private final Map<Integer, String> _hashes = new ConcurrentHashMap<>();

  public TraceWalCache(WalCache delegate) {
    _delegate = delegate;
  }

  @Override
  public boolean readBlocks(List<ReadRequest> requests) throws IOException {
    return TraceReader.trace(LOGGER, _delegate, _hashes, requests);
  }

  @Override
  public void write(long layer, int blockId, byte[] value) throws IOException {
    String md5AsBase64 = Md5Utils.md5AsBase64(value);
    _hashes.put(blockId, md5AsBase64);
    _delegate.write(layer, blockId, value);
  }

  @Override
  public long getMaxLayer() {
    return _delegate.getMaxLayer();
  }

  @Override
  public long getCreationTime() {
    return _delegate.getCreationTime();
  }

  @Override
  public void copy(Writer writer) throws IOException {
    _delegate.copy(writer);
  }

  @Override
  public long getId() {
    return _delegate.getId();
  }

  @Override
  public List<BlockReader> getLeaves() {
    return _delegate.getLeaves();
  }

  @Override
  public void close() throws IOException {
    _delegate.close();
  }

  @Override
  public int compareTo(WalCache o) {
    return _delegate.compareTo(o);
  }

  public static WalCache traceIfEnabled(WalCache walCache) {
    if (LOGGER.isTraceEnabled() || Boolean.getBoolean("pack.trace")) {
      return new TraceWalCache(walCache);
    }
    return walCache;
  }

  @Override
  public int getSize() {
    return _delegate.getSize();
  }

  @Override
  public boolean isClosed() {
    return _delegate.isClosed();
  }

  @Override
  public void incRef() {
    _delegate.incRef();
  }

  @Override
  public void decRef() {
    _delegate.decRef();
  }

  @Override
  public int refCount() {
    return _delegate.refCount();
  }

}
