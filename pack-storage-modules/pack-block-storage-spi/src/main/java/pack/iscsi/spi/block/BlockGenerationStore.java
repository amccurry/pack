package pack.iscsi.spi.block;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import pack.iscsi.spi.BlockKey;

public interface BlockGenerationStore extends Closeable {

  Map<BlockKey, Long> getAllLastStoredGeneration(long volumeId) throws IOException;

  long getLastStoredGeneration(long volumeId, long blockId) throws IOException;

  void setLastStoredGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException;

  default void close() throws IOException {

  }

  default void preloadGenerationInfo(long volumeId, long numberOfBlocks) throws IOException {

  }
}
