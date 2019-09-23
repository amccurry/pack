package pack.iscsi.volume.cache.wal;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.spi.block.BlockIORequest;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.wal.BlockJournalRange;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

public class BlockWriteAheadLogRecovery implements BlockIOExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockWriteAheadLogRecovery.class);

  @Value
  @Builder(toBuilder = true)
  public static class BlockWriteAheadLogRecoveryConfig {
    BlockWriteAheadLog blockWriteAheadLog;
  }

  private final BlockWriteAheadLog _blockWriteAheadLog;

  public BlockWriteAheadLogRecovery(BlockWriteAheadLogRecoveryConfig config) {
    _blockWriteAheadLog = config.getBlockWriteAheadLog();
  }

  @Override
  public BlockIOResponse exec(BlockIORequest request) throws IOException {
    long volumeId = request.getVolumeId();
    long blockId = request.getBlockId();
    long onDiskGeneration = request.getOnDiskGeneration();
    long lastStoredGeneration = request.getLastStoredGeneration();
    if (onDiskGeneration < lastStoredGeneration) {
      throw new IOException("volumeId " + volumeId + " blockId " + blockId + " on disk generation " + onDiskGeneration
          + " less than last store generation " + lastStoredGeneration + " something is wrong");
    }
    long generation = recover(request.getRandomAccessIO(), volumeId, blockId, request.getOnDiskGeneration());
    return BlockIOResponse.builder()
                          .lastStoredGeneration(lastStoredGeneration)
                          .onDiskBlockState(BlockState.DIRTY)
                          .onDiskGeneration(generation)
                          .build();
  }

  /**
   * Recover all changes from on disk generation and return the most generation
   * from the log.
   */
  public long recover(RandomAccessIO randomAccessIO, long volumeId, long blockId, long onDiskGeneration)
      throws IOException {
    List<BlockJournalRange> journalRanges = _blockWriteAheadLog.getJournalRanges(volumeId, blockId, onDiskGeneration,
        true);
    removeDuplicates(journalRanges);
    Collections.sort(journalRanges);
    checkForGaps(journalRanges);
    return applyJournals(randomAccessIO, journalRanges, onDiskGeneration);
  }

  private long applyJournals(RandomAccessIO randomAccessIO, List<BlockJournalRange> journalRanges,
      long onDiskGeneration) throws IOException {
    for (BlockJournalRange range : journalRanges) {
      onDiskGeneration = _blockWriteAheadLog.recoverFromJournal(randomAccessIO, range, onDiskGeneration);
    }
    return onDiskGeneration;
  }

  private void checkForGaps(List<BlockJournalRange> journalRanges) {
    LOGGER.info("todo check for gaps");
  }

  private void removeDuplicates(List<BlockJournalRange> journalRanges) {
    LOGGER.info("removeDuplicates and overlaps");
  }
}
