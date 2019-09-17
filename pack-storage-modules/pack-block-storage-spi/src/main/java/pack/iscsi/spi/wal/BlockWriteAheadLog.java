package pack.iscsi.spi.wal;

import java.io.Closeable;
import java.io.IOException;

import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.spi.block.BlockIORequest;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;

public interface BlockWriteAheadLog extends Closeable {

  @Override
  default void close() throws IOException {

  }

  default BlockIOExecutor getWriteAheadLogReader() {
    return new BlockIOExecutor() {
      @Override
      public BlockIOResponse exec(BlockIORequest request) throws IOException {
        long onDiskGeneration = request.getOnDiskGeneration();
        long lastStoredGeneration = request.getLastStoredGeneration();
        if (onDiskGeneration < lastStoredGeneration) {
          throw new IOException("On disk generation " + onDiskGeneration + " less than last store generation "
              + lastStoredGeneration + " something is wrong");
        }
        long volumeId = request.getVolumeId();
        long blockId = request.getBlockId();
        long generation = recover(request.getRandomAccessIO(), volumeId, blockId, request.getOnDiskGeneration());
        return BlockIOResponse.builder()
                              .lastStoredGeneration(lastStoredGeneration)
                              .onDiskBlockState(BlockState.DIRTY)
                              .onDiskGeneration(generation)
                              .build();
      }
    };
  }

  /**
   * Writes new data to a write ahead log for given generation returns a result.
   */
  BlockWriteAheadLogResult write(long volumeId, long blockId, long generation, long position, byte[] bytes, int offset,
      int len) throws IOException;

  /**
   * Writes new data to a write ahead log for given generation returns a result.
   */
  default BlockWriteAheadLogResult write(long volumeId, long blockId, long generation, long position, byte[] bytes)
      throws IOException {
    return write(volumeId, blockId, generation, position, bytes, 0, bytes.length);
  }

  /**
   * Release data from write ahead log before given generation inclusive (lower
   * generations).
   */
  void release(long volumeId, long blockId, long generation) throws IOException;

  /**
   * Recover all changes from on disk generation and return the most generation
   * from the log.
   */
  long recover(RandomAccessIO randomAccessIO, long volumeId, long blockId, long onDiskGeneration) throws IOException;

}
