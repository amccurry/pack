package pack.distributed.storage.monitor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackWriteBlockMonitor implements WriteBlockMonitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackWriteBlockMonitor.class);

  private final ConcurrentMap<Integer, List<Long>> _map;

  public PackWriteBlockMonitor() {
    _map = new ConcurrentHashMap<>();
  }

  @Override
  public long createTransId() {
    return System.nanoTime();
  }

  @Override
  public void resetDirtyBlock(int blockId, long transId) {
    List<Long> list = _map.get(blockId);
    if (list == null) {
      return;
    }
    synchronized (list) {
      list.remove(transId);
      list.notifyAll();
      if (list.isEmpty()) {
        _map.remove(blockId);
      }
    }
  }

  @Override
  public void addDirtyBlock(int blockId, long transId) {
    List<Long> value = new ArrayList<>();
    List<Long> list = _map.putIfAbsent(blockId, value);
    if (list == null) {
      list = value;
    }
    synchronized (list) {
      list.add(transId);
      list.notifyAll();
    }
  }

  @Override
  public void waitIfNeededForSync(int blockId) {
    List<Long> list = _map.get(blockId);
    if (list == null) {
      return;
    }
    while (true) {
      synchronized (list) {
        if (list.isEmpty()) {
          return;
        }
        try {
          LOGGER.info("Waiting for block to written to wal {}", blockId);
          list.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}
