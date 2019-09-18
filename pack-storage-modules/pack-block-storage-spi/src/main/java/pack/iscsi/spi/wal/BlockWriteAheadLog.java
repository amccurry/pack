package pack.iscsi.spi.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import pack.iscsi.spi.RandomAccessIO;

public interface BlockWriteAheadLog extends Closeable {

  @Override
  default void close() throws IOException {

  }

  /**
   * Writes new data to a write ahead log for given generation returns a result.
   */
  BlockJournalResult write(long volumeId, long blockId, long generation, long position, byte[] bytes, int offset,
      int len) throws IOException;

  /**
   * Writes new data to a write ahead log for given generation returns a result.
   */
  default BlockJournalResult write(long volumeId, long blockId, long generation, long position, byte[] bytes)
      throws IOException {
    return write(volumeId, blockId, generation, position, bytes, 0, bytes.length);
  }

  /**
   * Release data from write ahead log before given generation inclusive (lower
   * generations).
   */
  void releaseJournals(long volumeId, long blockId, long generation) throws IOException;

  List<BlockJournalRange> getJournalRanges(long volumeId, long blockId, long onDiskGeneration,
      boolean closeExistingWriter) throws IOException;

  default long recoverFromJournal(RandomAccessIO randomAccessIO, BlockJournalRange range, long onDiskGeneration)
      throws IOException {
    return recoverFromJournal(BlockRecoveryWriter.toBlockRecoveryWriter(randomAccessIO), range, onDiskGeneration);
  }

  long recoverFromJournal(BlockRecoveryWriter writer, BlockJournalRange range, long onDiskGeneration)
      throws IOException;

}
