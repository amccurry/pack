package pack.nativehdfs;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import jnr.ffi.Pointer;

public class FileReaderWriter implements Closeable {

  private final RandomAccessFile _rand;

  public FileReaderWriter(File file) throws IOException {
    _rand = new RandomAccessFile(file, "rw");
  }

  public synchronized void setLength(long size) throws IOException {
    _rand.setLength(size);
  }

  @Override
  public synchronized void close() throws IOException {
    _rand.close();
  }

  public synchronized int read(Pointer buf, long size, long offset) throws IOException {
    int len = (int) size;
    byte[] buffer = new byte[len];
    _rand.seek(offset);
    _rand.read(buffer, 0, len);
    buf.put(0, buffer, 0, len);
    return len;
  }

  public synchronized int write(Pointer buf, long size, long offset) throws IOException {
    int len = (int) size;
    byte[] buffer = new byte[len];
    buf.get(0, buffer, 0, len);
    _rand.seek(offset);
    _rand.write(buffer, 0, len);
    return len;
  }

}
