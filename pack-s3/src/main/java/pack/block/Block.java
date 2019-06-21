package pack.block;

import java.io.Closeable;

public interface Block extends Closeable {

  int read(long position, byte[] buf, int off, int len) throws Exception;

  int write(long position, byte[] buf, int off, int len) throws Exception;
  
  long getIdleTime();

  void sync();

  void close();

  default void readFully(long position, byte[] buf, int off, int len) throws Exception {
    while (len > 0) {
      int read = read(position, buf, off, len);
      len -= read;
      off += read;
      position += read;
    }
  }

  default void writeFully(long position, byte[] buf, int off, int len) throws Exception {
    while (len > 0) {
      int write = write(position, buf, off, len);
      len -= write;
      off += write;
      position += write;
    }
  }

}