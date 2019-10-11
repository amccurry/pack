package pack.iscsi.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

public class ConcurrentUtils {

  public static ExecutorService executor(String name, int threads) {
    return Executors.newFixedThreadPool(threads, new PackThreadFactory(name));
  }

  public static void sleep(TimeUnit unit, long sleep) {
    try {
      Thread.sleep(unit.toMillis(sleep));
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static <T> T runUntilSuccess(Logger logger, Callable<T> callable) {
    return runUntilSuccess(logger, callable, 1);
  }

  public static <T> T runUntilSuccess(Logger logger, Callable<T> callable, int maxRetries) {
    return runUntilSuccess(logger, callable, maxRetries, TimeUnit.SECONDS, 3);
  }

  public static <T> T runUntilSuccess(Logger logger, Callable<T> callable, int maxRetries, TimeUnit unit,
      long sleepBetweenRetries) {
    Exception lastError = null;
    for (int i = 0; i < maxRetries; i++) {
      try {
        return callable.call();
      } catch (Exception e) {
        lastError = e;
        logger.error("Unknown error, retry count " + i, e);
        ConcurrentUtils.sleep(unit, sleepBetweenRetries);
      }
    }
    if (lastError instanceof RuntimeException) {
      throw (RuntimeException) lastError;
    }
    throw new RuntimeException(lastError);
  }

  private static class PackThreadFactory implements ThreadFactory {
    private final ThreadGroup _group;
    private final AtomicInteger _threadNumber = new AtomicInteger(1);
    private final String _namePrefix;

    PackThreadFactory(String name) {
      _group = Thread.currentThread()
                     .getThreadGroup();
      _namePrefix = name + "-";
    }

    public Thread newThread(Runnable r) {
      Thread t = new Thread(_group, r, _namePrefix + _threadNumber.getAndIncrement(), 0);
      if (t.isDaemon())
        t.setDaemon(false);
      if (t.getPriority() != Thread.NORM_PRIORITY)
        t.setPriority(Thread.NORM_PRIORITY);
      return t;
    }
  }
}
