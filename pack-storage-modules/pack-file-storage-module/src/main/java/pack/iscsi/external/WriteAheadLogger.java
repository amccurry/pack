package pack.iscsi.external;

import java.io.Closeable;
import java.io.IOException;

import pack.iscsi.spi.RandomAccessIO;

public interface WriteAheadLogger extends Closeable {

  void append(long generation, long position, byte[] bytes, int offset, int len) throws IOException;

  /**
   * Recover the block from the given generation.
   */
  long recover(RandomAccessIO randomAccessIO, long onDiskGeneration) throws IOException;

  /**
   * Release entries after given generation.
   * 
   * @param generation
   * @throws IOException
   */
  void release(long generation) throws IOException;

}
