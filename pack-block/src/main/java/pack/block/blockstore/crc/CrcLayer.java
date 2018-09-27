package pack.block.blockstore.crc;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface CrcLayer extends Closeable {

  @Override
  default void close() throws IOException {

  }

  int getBlockSize();

  default void put(int blockId, ByteBuffer byteBuffer) {

  }

  default void put(int blockId, byte[] buf) {

  }

  default void validate(Object msg, int blockId, byte[] buf) {

  }

  default void validate(Object msg, int blockId, byte[] buf, int off, int len) {

  }

  default void validateDeleted(Object msg, int blockId) {

  }

  default void delete(int startingBlockId, int endingBlockId) {

  }

}