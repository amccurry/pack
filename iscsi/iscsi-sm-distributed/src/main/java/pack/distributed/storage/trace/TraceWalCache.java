package pack.distributed.storage.trace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.Md5Utils;

import pack.distributed.storage.BlockReader;
import pack.distributed.storage.hdfs.BlockFile.Writer;
import pack.distributed.storage.hdfs.ReadRequest;
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
    boolean[] completedBefore = new boolean[requests.size()];
    for (int i = 0; i < requests.size(); i++) {
      completedBefore[i] = requests.get(i)
                                   .isCompleted();
    }
    boolean readBlocks = _delegate.readBlocks(requests);
    for (int i = 0; i < requests.size(); i++) {
      if (!completedBefore[i]) {
        ReadRequest readRequest = requests.get(i);
        int blockId = readRequest.getBlockId();
        if (blockId != 0L) {
          continue;
        }
        String current = _hashes.get(blockId);
        LOGGER.info("Read {} {}", blockId, current);
        if (readRequest.isCompleted()) {
          // Complete during this readBlocks call
          if (current != null) {
            ByteBuffer duplicate = readRequest.getByteBuffer()
                                              .duplicate();
            duplicate.flip();
            byte[] value = new byte[duplicate.remaining()];
            duplicate.get(value);
            String bs = Md5Utils.md5AsBase64(value);
            if (!current.equals(bs)) {
              LOGGER.error("Bid {} not equal {} {} {}", blockId, current, bs, value.length);
            }
          } else {
            LOGGER.error("Wal Cache HIT when shouldn't {}", blockId);
          }
        } else {
          // Not completed during this readBlocks call
          if (current != null) {
            LOGGER.error("Wal Cache MISS when shouldn't {} {}", blockId, current);
          }
        }
      }
    }
    return readBlocks;
  }

  @Override
  public void write(long layer, int blockId, byte[] value) throws IOException {
    String md5AsBase64 = Md5Utils.md5AsBase64(value);
    if (blockId == 0) {
      LOGGER.info("Write {} {}", blockId, md5AsBase64);
    }
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

}
