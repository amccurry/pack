package pack.iscsi.spi.block;

import java.io.Closeable;
import java.io.IOException;

public interface BlockGenerationStore extends Closeable {

  long getLastStoreGeneration(long volumeId, long blockId) throws IOException;

  void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException;

  default void close() throws IOException {
    
  }
}
