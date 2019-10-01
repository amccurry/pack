package pack.iscsi.spi.wal;

import java.io.IOException;

import pack.iscsi.spi.RandomAccessIO;

public interface BlockRecoveryWriter {

  public static BlockRecoveryWriter toBlockRecoveryWriter(long startingPositionOfBlock, RandomAccessIO randomAccessIO) {
    return (generation, position, buffer, offset, length) -> {
      randomAccessIO.writeFully(startingPositionOfBlock + position, buffer, offset, length);
      return true;
    };
  }

  /**
   * Write entry and return boolean indicating whether the
   * {@link BlockRecoveryWriter} can handle more input.
   */
  default boolean writeEntry(long generation, long position, byte[] buffer) throws IOException {
    return writeEntry(generation, position, buffer, 0, buffer.length);
  }

  /**
   * Write entry and return boolean indicating whether the
   * {@link BlockRecoveryWriter} can handle more input.
   */
  boolean writeEntry(long generation, long position, byte[] buffer, int offset, int length) throws IOException;

}
