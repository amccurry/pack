package pack.block.blockstore;

import java.io.Closeable;
import java.io.IOException;

import pack.block.util.PackSizeOf;

public interface BlockStore extends Closeable, PackSizeOf {

  /**
   * Name of the BlockStore, must be a valid filename.
   * 
   * @return
   */
  String getName() throws IOException;

  /**
   * Length of BlockStore in bytes.
   * 
   * @return
   */
  long getLength() throws IOException;

  /**
   * Last modified time, not required (return 0).
   * 
   * @return
   */
  long lastModified() throws IOException;

  int write(long position, byte[] buffer, int offset, int len) throws IOException;

  int read(long position, byte[] buffer, int offset, int len) throws IOException;

  void fsync() throws IOException;

  void delete(long position, long length) throws IOException;

  BlockStoreMetaData getMetaData() throws IOException;

}
