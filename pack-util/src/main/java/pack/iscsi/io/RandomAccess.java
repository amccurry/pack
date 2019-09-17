package pack.iscsi.io;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface RandomAccess extends DataInput, DataOutput, Closeable {

  void seek(long position) throws IOException;

  long getFilePointer() throws IOException;

  public long length() throws IOException;

  void setLength(long newLength) throws IOException;

  int read() throws IOException;

  int read(byte[] b, int off, int len) throws IOException;

  int read(byte[] b) throws IOException;
}
