package pack.block.blockstore;

import java.io.Closeable;
import java.io.IOException;

import pack.block.server.fs.LinuxFileSystem;

public interface BlockStore extends Closeable {

  LinuxFileSystem getLinuxFileSystem();
  
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

  int write(long position, byte[] buffer, int offset, int len) throws IOException;

  int read(long position, byte[] buffer, int offset, int len) throws IOException;

  void fsync() throws IOException;

}
