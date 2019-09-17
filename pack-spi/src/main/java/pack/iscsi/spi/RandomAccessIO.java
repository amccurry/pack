package pack.iscsi.spi;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface RandomAccessIO extends DataInput, DataOutput, Closeable {

  default void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  void readFully(long position, byte[] buffer, int offset, int length) throws IOException;

  default void writeFully(long position, byte[] buffer) throws IOException {
    writeFully(position, buffer, 0, buffer.length);
  }

  void writeFully(long position, byte[] buffer, int offset, int length) throws IOException;

  void seek(long position) throws IOException;

  long getFilePointer() throws IOException;

  public long length() throws IOException;

  void setLength(long newLength) throws IOException;

  int read() throws IOException;

  int read(byte[] b, int off, int len) throws IOException;

  default int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

}
