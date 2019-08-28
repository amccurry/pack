package pack.iscsi.external.local;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;

public interface Log extends Closeable {

  void append(long generation, long position, byte[] bytes, int offset, int len) throws IOException;

  /**
   * Recover the block from the given generation.
   * 
   * @param channel
   * @param onDiskGeneration
   * @return
   * @throws IOException
   */
  long recover(FileChannel channel, long onDiskGeneration) throws IOException;

  /**
   * Release entries after given generation.
   * 
   * @param generation
   * @throws IOException
   */
  void release(long generation) throws IOException;

}
