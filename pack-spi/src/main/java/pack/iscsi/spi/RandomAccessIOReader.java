package pack.iscsi.spi;

import java.io.Closeable;
import java.io.IOException;

public interface RandomAccessIOReader extends Closeable {

  default void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  void readFully(long position, byte[] buffer, int offset, int length) throws IOException;

  long length() throws IOException;
}
