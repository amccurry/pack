package pack.iscsi.volume;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.spi.StorageModule;

public class TimerStorageModule implements StorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimerStorageModule.class);

  private final StorageModule _storageModule;
  private final Timer _timer;

  public TimerStorageModule(StorageModule storageModule) {
    _storageModule = storageModule;
    _timer = new Timer("timer-thread-watcher", true);
  }

  public int checkBounds(long logicalBlockAddress, int transferLengthInBlocks) {
    return _storageModule.checkBounds(logicalBlockAddress, transferLengthInBlocks);
  }

  public long getSizeInBlocks() {
    return _storageModule.getSizeInBlocks();
  }

  public void read(byte[] bytes, long storageIndex) throws IOException {
    Thread thread = Thread.currentThread();
    long start = System.currentTimeMillis();
    TimerTask task = getTask(thread, start);
    _timer.schedule(task, TimeUnit.SECONDS.toMillis(3), TimeUnit.SECONDS.toMillis(3));
    try {
      _storageModule.read(bytes, storageIndex);
    } finally {
      task.cancel();
      _timer.purge();
    }
  }

  public void write(byte[] bytes, long storageIndex) throws IOException {
    Thread thread = Thread.currentThread();
    long start = System.currentTimeMillis();
    TimerTask task = getTask(thread, start);
    _timer.schedule(task, TimeUnit.SECONDS.toMillis(3), TimeUnit.SECONDS.toMillis(3));
    try {
      _storageModule.write(bytes, storageIndex);
    } finally {
      task.cancel();
      _timer.purge();
    }
  }

  public void close() throws IOException {
    _storageModule.close();
  }

  public void flushWrites() throws IOException {
    Thread thread = Thread.currentThread();
    long start = System.currentTimeMillis();
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        LOGGER.info("Thread {} still running {} ms stack {}", thread, (System.currentTimeMillis() - start),
            thread.getStackTrace());
      }
    };
    _timer.schedule(task, TimeUnit.SECONDS.toMillis(3), TimeUnit.SECONDS.toMillis(3));
    try {
      _storageModule.flushWrites();
    } finally {
      task.cancel();
      _timer.purge();
    }
  }

  public int getBlockSize() {
    return _storageModule.getBlockSize();
  }

  public long getBlocks(long sizeInBytes) {
    return _storageModule.getBlocks(sizeInBytes);
  }

  public long getSizeInBytes() {
    return _storageModule.getSizeInBytes();
  }

  private TimerTask getTask(Thread thread, long start) {
    return new TimerTask() {
      @Override
      public void run() {
        StringWriter writer = new StringWriter();
        try (PrintWriter pw = new PrintWriter(writer)) {
          pw.println();
          pw.println(this);
          StackTraceElement[] trace = thread.getStackTrace();
          for (StackTraceElement traceElement : trace) {
            pw.println("\tat " + traceElement);
          }
        }
        LOGGER.info("Thread {} still running {} ms stack {}", thread, (System.currentTimeMillis() - start),
            writer.toString());
      }
    };
  }
}
