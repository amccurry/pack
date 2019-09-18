package pack.iscsi.spi;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;

public interface StorageModule extends Closeable {
  public static final int VIRTUAL_BLOCK_SIZE = 512;

  /**
   * This method can be used for checking if a (series of) I/O operations will
   * result in an {@link IOException} due to trying to access blocks outside the
   * medium's boundaries.
   * <p>
   * The SCSI standard requires checking for these boundary violations right
   * after receiving a read or write command, so that an appropriate error
   * message can be returned to the initiator. Therefore this method must be
   * called prior to each read or write sequence.
   * <p>
   * The values returned by this method and their meaning with regard to the
   * interval [0, {@link #getSizeInBlocks()} - 1] are shown in the following
   * table:
   * <p>
   * <table border="1">
   * <tr>
   * <th>Return Value</th>
   * <th>Meaning</th>
   * </tr>
   * <tr>
   * <td>0</td>
   * <td>no boundaries are violated</td>
   * </tr>
   * <tr>
   * <td>1</td>
   * <td>the <i>logicalBlockAddress</i> parameter lies outside of the
   * interval</td>
   * </tr>
   * <tr>
   * <td>2</td>
   * <td>the interval [<i>logicalBlockAddress</i>, <i>logicalBlockAddress</i> +
   * <i>transferLengthInBlocks</i>]<br/>
   * lies outside of the interval, or <i>transferLengthInBlocks</i> is
   * negative</td>
   * </tr>
   * </table>
   * <p>
   * Note that the parameters of this method are referring to blocks, not to
   * byte indices.
   * 
   * @param logicalBlockAddress
   *          the index of the first block of data to be read or written
   * @param transferLengthInBlocks
   *          the total number of consecutive blocks about to be read or written
   * @return see table in description
   */
  default int checkBounds(final long logicalBlockAddress, final int transferLengthInBlocks) {
    long blocks = getBlocks(getSizeInBytes());
    if (logicalBlockAddress < 0 || logicalBlockAddress > blocks) {
      return 1;
    } else if (transferLengthInBlocks < 0 || logicalBlockAddress + transferLengthInBlocks > blocks) {
      return 2;
    } else {
      return 0;
    }
  }

  /**
   * Returns the storage space size in bytes divided by the block size in bytes
   * (rounded down).
   * 
   * @return the virtual amount of storage blocks available
   */
  default long getSizeInBlocks() {
    return getBlocks(getSizeInBytes()) - 1;
  }

  /**
   * Copies bytes from storage to the passed byte array.
   * 
   * @param bytes
   *          the array into which the data will be copied will be filled with
   *          data from storage
   * @param storageIndex
   *          the position of the first byte to be copied
   * @throws IOException
   */
  void read(byte[] bytes, long storageIndex) throws IOException;

  default void read(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber) throws IOException {
    read(bytes, storageIndex);
  }

  /**
   * Saves part of the passed byte array's content.
   * 
   * @param pdu
   * 
   * @param bytes
   *          the source of the data to be stored
   * @param storageIndex
   *          byte offset in the storage area
   * @throws IOException
   */
  void write(byte[] bytes, long storageIndex) throws IOException;

  default void write(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber, Integer dataSequenceNumber, Integer targetTransferTag) throws IOException {
    write(bytes, storageIndex);
  }

  /**
   * Closing the storage.
   * 
   * @throws IOException
   *           to be closed
   */
  void close() throws IOException;

  default void flushWrites() throws IOException {

  }

  default int getBlockSize() {
    return VIRTUAL_BLOCK_SIZE;
  }

  default long getBlocks(long sizeInBytes) {
    return sizeInBytes / getBlockSize();
  }

  long getSizeInBytes();

}