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

import com.amazonaws.util.Md5Utils;

import pack.distributed.storage.hdfs.BlockFile.Writer;
import pack.distributed.storage.hdfs.ReadRequest;

public class InMemoryWalCache implements WalCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryWalCache.class);

  private final long _ts = System.currentTimeMillis();
  private final long _layer;
  private final AtomicLong _maxLayer = new AtomicLong();
  private final Map<Integer, byte[]> _cache = new ConcurrentHashMap<>();

  public InMemoryWalCache(long layer) {
    _layer = layer;
    setMaxlayer(layer);
  }

  @Override
  public boolean readBlocks(List<ReadRequest> requests) throws IOException {
    boolean more = false;
    for (ReadRequest readRequest : requests) {
      int blockId = readRequest.getBlockId();
      if (!readRequest.isCompleted()) {
        byte[] bs = _cache.get(blockId);
        if (bs != null) {
          if (blockId == 0) {
            LOGGER.info("read {} {}", blockId, Md5Utils.md5AsBase64(bs));
          }
          readRequest.handleResult(bs);
        } else {
          if (blockId == 0) {
            LOGGER.info("read miss {}", blockId);
          }
          more = true;
        }
      } else {
        if (blockId == 0) {
          LOGGER.info("read complete {}", blockId);
        }
      }
    }
    return more;
  }

  @Override
  public void write(long layer, int blockId, byte[] block) throws IOException {
    if (blockId == 0) {
      LOGGER.info("write {} {}", blockId, Md5Utils.md5AsBase64(block));
    }
    setMaxlayer(layer);
    _cache.put(blockId, block);
  }

  @Override
  public void copy(Writer writer) throws IOException {
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
