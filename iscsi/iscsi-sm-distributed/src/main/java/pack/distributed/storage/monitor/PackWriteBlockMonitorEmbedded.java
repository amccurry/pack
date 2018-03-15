package pack.distributed.storage.monitor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackWriteBlockMonitorEmbedded implements WriteBlockMonitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackWriteBlockMonitorEmbedded.class);

  private final ConcurrentMap<Integer, List<Long>> _map;
  private final long _maxWait;

  public PackWriteBlockMonitorEmbedded(String name) {
    _map = new ConcurrentHashMap<>();
    _maxWait = TimeUnit.MILLISECONDS.toMillis(100);
  }

  @Override
  public void clearAllLocks() {
    _map.clear();
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
  public boolean waitIfNeededForSync(int blockId) {
    List<Long> list = _map.get(blockId);
    if (list == null) {
      return false;
    }
    while (true) {
      synchronized (list) {
        if (list.isEmpty()) {
          return false;
        }
        try {
          LOGGER.info("Waiting for block to written to wal block {} transIds {}", blockId, list);
          list.wait(_maxWait);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        if (list.isEmpty()) {
          return false;
        } else {
          return true;
        }
      }
    }
  }

}
