package pack.iscsi.spi;

import java.io.Closeable;
import java.io.IOException;

public interface RandomAccessIOReader extends Closeable {

  default void read(long position, byte[] buffer) throws IOException {
    read(position, buffer, 0, buffer.length);
  }

  void read(long position, byte[] buffer, int offset, int length) throws IOException;

  long length() throws IOException;
}
