package pack.distributed.storage.wal;

import java.io.File;
import java.io.IOException;

import pack.distributed.storage.PackMetaData;

public class PackWalCacheFactory implements WalCacheFactory {

  private final PackMetaData _metaData;
  private final File _cacheDir;

  public PackWalCacheFactory(PackMetaData metaData, File cacheDir) {
    _metaData = metaData;
    _cacheDir = cacheDir;
  }

  @Override
  public WalCache create(long layer) throws IOException {
    return new PackWalCache(_cacheDir, layer, _metaData.getLength(), _metaData.getBlockSize());
  }

}
