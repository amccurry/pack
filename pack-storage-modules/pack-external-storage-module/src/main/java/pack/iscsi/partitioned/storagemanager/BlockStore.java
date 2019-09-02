package pack.iscsi.partitioned.storagemanager;

import java.io.Closeable;
import java.io.IOException;

public interface BlockStore extends Closeable {

  long getLastStoreGeneration(long volumeId, long blockId) throws IOException;

  void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException;

  default void close() throws IOException {
    
  }
}
