package pack.iscsi.partitioned.block;

import java.io.Closeable;
import java.io.IOException;

public interface Block extends Closeable {

  public static long MISSING_BLOCK_GENERATION = 0;

  /**
   * Position is relative to the block.
   */
  void readFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException;

  /**
   * Position is relative to the block.
   */
  void writeFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException;

  void execIO(BlockIOExecutor executor) throws IOException;

  long getBlockId();

  long getVolumeId();

  BlockState getOnDiskState();

  long getOnDiskGeneration();

  long getLastStoredGeneration();

  int getSize();

  void cleanUp() throws IOException;

}