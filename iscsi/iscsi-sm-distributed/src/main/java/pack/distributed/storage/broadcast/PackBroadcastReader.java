package pack.distributed.storage.broadcast;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.wal.WalCacheManager;

public abstract class PackBroadcastReader implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackBroadcastReader.class);

  private final Thread _readerThread;
  private final AtomicBoolean _running = new AtomicBoolean(true);

  public PackBroadcastReader(String name, WalCacheManager walCacheManager) {
    _readerThread = new Thread(() -> {
      while (isRunning()) {
        try {
          writeDataToWal(walCacheManager);
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
      }
    });
    _readerThread.setDaemon(true);
    _readerThread.setName("PackBroadcastReader-" + name);
  }

  public boolean isRunning() {
    return _running.get();
  }

  public void start() {
    _readerThread.start();
  }

  /**
   * Waits for broadcast to become visible in WAL.
   * 
   * @throws IOException
   */
  public abstract void sync() throws IOException;

  protected abstract void writeDataToWal(WalCacheManager walCacheManager) throws IOException;

  @Override
  public void close() throws IOException {
    _running.set(false);
    _readerThread.interrupt();
  }

}
