package pack.block.blockstore.hdfs.lock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import pack.block.util.PackSizeOf;

public class FileLock implements PackLock {

  private final File _file;
  private RandomAccessFile _rand;
  private FileChannel _fc;
  private java.nio.channels.FileLock _lock;

  public static void main(String[] args) throws IOException {
    try (FileLock lock = new FileLock("./test.lock")) {
      if (lock.tryToLock()) {
        System.out.println("yay!");
      }
      try (FileLock lock2 = new FileLock("./test.lock")) {
        if (lock2.tryToLock()) {
          System.out.println("boo!");
        } else {
          System.out.println("yay!");
        }
      }
    }
  }

  public FileLock(String path) {
    _file = new File(path);
    _file.getParentFile()
         .mkdirs();
  }

  @Override
  public void close() throws IOException {
    boolean lockOwner = false;
    if (_lock != null) {
      _lock.close();
      lockOwner = true;
    }
    if (_fc != null) {
      _fc.close();
    }
    if (_rand != null) {
      _rand.close();
    }
    if (_rand != null) {
      _rand.close();
    }
    if (lockOwner) {
      _file.delete();
    }
  }

  @Override
  public boolean isLockOwner() throws IOException {
    return _lock != null;
  }

  @Override
  public boolean tryToLock() throws IOException {
    try {
      _rand = new RandomAccessFile(_file, "rw");
      _fc = _rand.getChannel();
      _lock = _fc.tryLock();
    } catch (Exception e) {
      if (_lock != null) {
        _lock.close();
      }
      if (_fc != null) {
        _fc.close();
      }
      if (_rand != null) {
        _rand.close();
      }
      return false;
    }
    return _lock != null;
  }

  public static boolean isLocked(String path) throws IOException, InterruptedException {
    try (FileLock lock = new FileLock(path)) {
      return !lock.tryToLock();
    }
  }

  @Override
  public long getSizeOf() {
    return PackSizeOf.getSizeOfObject(this, true);
  }
}
