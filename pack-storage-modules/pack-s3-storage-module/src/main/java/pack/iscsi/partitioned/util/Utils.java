package pack.iscsi.partitioned.util;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

public class Utils {

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
}
