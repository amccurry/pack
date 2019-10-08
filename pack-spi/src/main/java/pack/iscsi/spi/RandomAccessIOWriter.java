package pack.iscsi.spi;

import java.io.Closeable;
import java.io.IOException;

public interface RandomAccessIOWriter extends Closeable {

  default void write(long position, byte[] buffer) throws IOException {
    write(position, buffer, 0, buffer.length);
  }

  void write(long position, byte[] buffer, int offset, int length) throws IOException;

  default void punchHole(long position, long length) throws IOException {

  }

  void setLength(long length) throws IOException;

  void flush() throws IOException;

}
