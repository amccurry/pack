package pack.iscsi.spi.block;

import java.io.IOException;

public interface BlockCacheMetadataStore {

  default void setCachedBlockIds(long volumeId, long... blockIds) throws IOException {

  }

  default long[] getCachedBlockIds(long volumeId) throws IOException {
    return new long[] {};
  }

}
