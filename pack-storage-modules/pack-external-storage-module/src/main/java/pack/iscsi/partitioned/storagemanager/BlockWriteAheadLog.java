package pack.iscsi.partitioned.storagemanager;

import java.io.IOException;
import java.nio.channels.FileChannel;

import pack.iscsi.partitioned.block.BlockIOExecutor;
import pack.iscsi.partitioned.block.BlockIORequest;
import pack.iscsi.partitioned.block.BlockIOResponse;
import pack.iscsi.partitioned.block.BlockState;

public interface BlockWriteAheadLog {

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
        long generation = recover(request.getChannel(), volumeId, blockId, request.getOnDiskGeneration());
        return BlockIOResponse.builder()
                              .lastStoredGeneration(lastStoredGeneration)
                              .onDiskBlockState(BlockState.DIRTY)
                              .onDiskGeneration(generation)
                              .build();
      }
    };
  }

  /**
   * Writes new data to a write ahead log for given generation.
   */
  void write(long volumeId, long blockId, long generation, long position, byte[] bytes, int offset, int len)
      throws IOException;

  /**
   * Release data from write ahead log before given generation (lower
   * generations).
   */
  void release(long volumeId, long blockId, long generation) throws IOException;

  /**
   * Recover all changes from on disk generation and return the most generation
   * from the log.
   */
  long recover(FileChannel channel, long volumeId, long blockId, long onDiskGeneration) throws IOException;

}
