package pack.iscsi.spi.block;

import java.io.Closeable;
import java.io.IOException;

import pack.iscsi.spi.async.AsyncCompletableFuture;

public interface Block extends Closeable {

  public static long MISSING_BLOCK_GENERATION = 0;

  /**
   * Position is relative to the block.
   */
  void readFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException;

  /**
   * Position is relative to the block.
   */
  default AsyncCompletableFuture writeFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException {
    return writeFully(blockPosition, bytes, offset, len, true);
  }

  /**
   * Position is relative to the block.
   */
  AsyncCompletableFuture writeFully(long blockPosition, byte[] bytes, int offset, int len, boolean autoFlush)
      throws IOException;

  void execIO(BlockIOExecutor executor) throws IOException;

  long getBlockId();

  long getVolumeId();

  BlockState getOnDiskState();

  long getOnDiskGeneration();

  long getLastStoredGeneration();

  int getSize();

  boolean idleWrites();

  boolean isClosed();

}