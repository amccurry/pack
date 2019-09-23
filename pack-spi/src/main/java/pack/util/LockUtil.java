package pack.util;

import java.io.Closeable;
import java.util.concurrent.locks.Lock;

public class LockUtil {
  public static Closeable getCloseableLock(Lock lock) {
    lock.lock();
    return () -> lock.unlock();
  }
}
