package pack.util;

import java.io.Closeable;
import java.util.concurrent.locks.Lock;

public class PackLock implements Closeable {

  public static PackLock create(Lock lock) {
    return new PackLock(lock);
  }

  private final Lock _lock;

  private PackLock(Lock lock) {
    lock.lock();
    _lock = lock;
  }

  @Override
  public void close() {
    _lock.unlock();
  }

}
