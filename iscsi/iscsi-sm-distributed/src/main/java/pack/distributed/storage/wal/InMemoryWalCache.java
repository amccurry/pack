package pack.distributed.storage.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.hdfs.BlockFile.Writer;
import pack.distributed.storage.hdfs.ReadRequest;

public class InMemoryWalCache implements WalCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryWalCache.class);

  private final long _ts = System.currentTimeMillis();
  private final long _layer;
  private final AtomicLong _maxLayer = new AtomicLong();
  private final Map<Integer, byte[]> _cache = new ConcurrentHashMap<>();
  private final int _blockSize;

  public InMemoryWalCache(long layer, int blockSize) {
    _blockSize = blockSize;
    _layer = layer;
    setMaxlayer(layer);
  }

  @Override
  public int getSize() {
    return _cache.size() * _blockSize;
  }

  @Override
  public boolean readBlocks(List<ReadRequest> requests) throws IOException {
    boolean more = false;
    for (ReadRequest readRequest : requests) {
      int blockId = readRequest.getBlockId();
      if (!readRequest.isCompleted()) {
        byte[] bs = _cache.get(blockId);
        if (bs != null) {
          readRequest.handleResult(bs);
        } else {
          more = true;
        }
      }
    }
    return more;
  }

  @Override
  public void write(long layer, int blockId, byte[] block) throws IOException {
    setMaxlayer(layer);
    _cache.put(blockId, block);
  }

  @Override
  public void copy(Writer writer) throws IOException {
    LOGGER.debug("Copy to writer {} {}", this, writer);
    List<Integer> list = new ArrayList<>(_cache.keySet());
    Collections.sort(list);
    for (Integer blockId : list) {
      writer.append(blockId, new BytesWritable(_cache.get(blockId)));
    }
  }

  @Override
  public long getMaxLayer() {
    return _maxLayer.get();
  }

  @Override
  public long getCreationTime() {
    return _ts;
  }

  @Override
  public long getId() {
    return _layer;
  }

  private void setMaxlayer(long layer) {
    _maxLayer.set(layer);
  }
}
