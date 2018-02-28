package pack.iscsi.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.iscsi.storage.utils.IOUtils;

public class DelayedResourceCleanup extends TimerTask implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DelayedResourceCleanup.class);

  private final long _timeDelay;
  private final Map<Object, ResourceEntry> _entries = new ConcurrentHashMap<>();
  private final Timer _timer;

  public DelayedResourceCleanup(TimeUnit timeUnit, long timeDelay) {
    _timeDelay = timeUnit.toMillis(timeDelay);
    _timer = new Timer("DelayedResourceCleanup", true);
    _timer.schedule(this, _timeDelay / 2, _timeDelay / 2);
  }

  public boolean contains(Object key) {
    return _entries.containsKey(key);
  }

  public void register(Object key, Closeable... closeables) {
    if (closeables == null) {
      return;
    }
    ResourceEntry resourceEntry = new ResourceEntry(System.currentTimeMillis() + _timeDelay, closeables);
    _entries.put(key, resourceEntry);
  }

  static class ResourceEntry {
    long _cleanupTime;
    Closeable[] _closeables;

    ResourceEntry(long cleanupTime, Closeable[] closeables) {
      _cleanupTime = cleanupTime;
      _closeables = closeables;
    }

    boolean tryToCleanup() {
      if (_cleanupTime < System.currentTimeMillis()) {
        IOUtils.close(LOGGER, _closeables);
        return true;
      }
      return false;
    }
  }

  @Override
  public void run() {
    try {
      Builder<Object> builder = ImmutableList.builder();
      for (Entry<Object, ResourceEntry> e : _entries.entrySet()) {
        if (e.getValue()
             .tryToCleanup()) {
          builder.add(e.getKey());
        }
      }
      for (Object key : builder.build()) {
        _entries.remove(key);
      }
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
    }
  }

  @Override
  public void close() throws IOException {
    _timer.purge();
    _timer.cancel();
  }
}
