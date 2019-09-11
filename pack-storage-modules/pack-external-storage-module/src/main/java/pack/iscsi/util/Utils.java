package pack.iscsi.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.github.benmanes.caffeine.cache.LoadingCache;

public class Utils {

  private static final String MAXIMUM = "maximum";
  private static final String SET_MAXIMUM = "setMaximum";
  private static final String CACHE = "cache";

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
    return runUntilSuccess(logger, callable, 20);
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
        Utils.sleep(unit, sleepBetweenRetries);
      }
    }
    if (lastError instanceof RuntimeException) {
      throw (RuntimeException) lastError;
    }
    throw new RuntimeException(lastError);
  }

  public static void setMaximum(LoadingCache<?, ?> cache, long maximum) {
    try {
      Field declaredField = getField(cache.getClass(), CACHE);
      declaredField.setAccessible(true);
      Object internalCache = declaredField.get(cache);
      Method declaredMethodSetMaximum = getMethod(internalCache.getClass(), SET_MAXIMUM, Long.TYPE);
      declaredMethodSetMaximum.setAccessible(true);
      declaredMethodSetMaximum.invoke(internalCache, new Object[] { maximum });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static long getMaximum(LoadingCache<?, ?> cache) {
    try {
      Field declaredField = getField(cache.getClass(), CACHE);
      declaredField.setAccessible(true);
      Object internalCache = declaredField.get(cache);
      Method declaredMethodMaximum = getMethod(internalCache.getClass(), MAXIMUM);
      declaredMethodMaximum.setAccessible(true);
      return (long) declaredMethodMaximum.invoke(internalCache);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Method getMethod(Class<?> clazz, String name, Class<?>... parameterTypes) {
    try {
      return clazz.getDeclaredMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      Class<?> superclass = clazz.getSuperclass();
      if (superclass.equals(Object.class)) {
        return null;
      }
      return getMethod(superclass, name, parameterTypes);
    }
  }

  private static Field getField(Class<?> clazz, String name) throws Exception {
    try {
      return clazz.getDeclaredField(name);
    } catch (NoSuchFieldException e) {
      Class<?> superclass = clazz.getSuperclass();
      if (superclass.equals(Object.class)) {
        return null;
      }
      return getField(superclass, name);
    }
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
