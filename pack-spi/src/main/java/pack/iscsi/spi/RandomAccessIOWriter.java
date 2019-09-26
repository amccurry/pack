package pack.iscsi.spi;

import java.io.Closeable;
import java.io.IOException;

public interface RandomAccessIOWriter extends Closeable {

  default void writeFully(long position, byte[] buffer) throws IOException {
    writeFully(position, buffer, 0, buffer.length);
  }

  void writeFully(long position, byte[] buffer, int offset, int length) throws IOException;

  default void punchHole(long position, long length) throws IOException {

  }

}
