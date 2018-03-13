package pack.distributed.storage.read;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public interface BlockReader extends Closeable {

  BlockReader NOOP_READER = new BlockReader() {

    @Override
    public boolean readBlocks(List<ReadRequest> requests) throws IOException {
      if (requests.isEmpty()) {
        return false;
      }
      for (ReadRequest readRequest : requests) {
        if (!readRequest.isCompleted()) {
          // if not completed there is more work
          return true;
        }
      }
      return false;
    }

    @Override
    public String toString() {
      return "NOOP_READER";
    }

  };

  default List<BlockReader> getLeaves() {
    return Arrays.asList(this);
  }

  /**
   * Tries to read from the block to fulfill the read request. Return true if
   * there are more read requests that have not been fulfilled.
   * 
   * @param requests
   * @return true if there are more work to done, false if complete.
   * @throws IOException
   */
  boolean readBlocks(List<ReadRequest> requests) throws IOException;

  /**
   * Tries to read from the block to fulfill the read request. Return true if
   * there are more read requests that have not been fulfilled.
   * 
   * @param requests
   * @return true if there are more work to done, false if complete.
   * @throws IOException
   */
  default boolean readBlocks(ReadRequest... requests) throws IOException {
    if (requests == null) {
      return false;
    }
    return readBlocks(Arrays.asList(requests));
  }

  @Override
  default void close() throws IOException {

  }

  public static BlockReader mergeInOrder(List<? extends BlockReader> readers) {
    if (readers == null || readers.isEmpty()) {
      return NOOP_READER;
    }
    return new ListBlockReader(readers);
  }

  public static BlockReader mergeInOrder(BlockReader... readers) {
    if (readers == null) {
      return null;
    }
    return mergeInOrder(Arrays.asList(readers));
  }

  static class ListBlockReader implements BlockReader {

    private final ImmutableList<? extends BlockReader> _readers;

    public ListBlockReader(List<? extends BlockReader> readers) {
      _readers = ImmutableList.copyOf(readers);
    }

    @Override
    public boolean readBlocks(List<ReadRequest> requests) throws IOException {
      for (BlockReader reader : _readers) {
        if (!reader.readBlocks(requests)) {
          // return false if no more work
          return false;
        }
      }
      return true;
    }

    @Override
    public List<BlockReader> getLeaves() {
      Builder<BlockReader> builder = ImmutableList.builder();
      for (BlockReader blockReader : _readers) {
        builder.addAll(blockReader.getLeaves());
      }
      return builder.build();
    }

    @Override
    public String toString() {
      return "{" + _readers + "}";
    }

  }

}
