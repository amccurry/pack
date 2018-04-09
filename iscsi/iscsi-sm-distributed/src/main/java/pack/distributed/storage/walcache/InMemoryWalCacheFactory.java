package pack.distributed.storage.walcache;

import java.io.IOException;

import pack.distributed.storage.PackMetaData;

public class InMemoryWalCacheFactory implements WalCacheFactory {

  private final PackMetaData _metaData;

  public InMemoryWalCacheFactory(PackMetaData metaData) {
    _metaData = metaData;
  }

  @Override
  public WalCache create(long layer) throws IOException {
    return new InMemoryWalCache(layer, _metaData.getBlockSize());
  }

}
