package pack.distributed.storage.wal;

import java.io.IOException;

import pack.distributed.storage.PackMetaData;

public class InMemoryWalCacheFactory implements WalCacheFactory {

  public InMemoryWalCacheFactory(PackMetaData metaData) {
  }

  @Override
  public WalCache create(long layer) throws IOException {
    return new InMemoryWalCache(layer);
  }

}
