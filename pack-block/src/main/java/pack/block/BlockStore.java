package pack.block;

import java.io.Closeable;
import java.io.IOException;

import jnr.ffi.Pointer;

public interface BlockStore extends Closeable {

  /**
   * Name of the BlockStore, must be a valid filename.
   * 
   * @return
   */
  String getName();

  /**
   * Length of BlockStore in bytes.
   * 
   * @return
   */
  long getLength();

  /**
   * Last modified time, not required (return 0).
   * 
   * @return
   */
  long lastModified();

  int write(long position, Pointer buffer, int offset, int len) throws IOException;

  int read(long position, Pointer buffer, int offset, int len) throws IOException;

  void fsync() throws IOException;

}
