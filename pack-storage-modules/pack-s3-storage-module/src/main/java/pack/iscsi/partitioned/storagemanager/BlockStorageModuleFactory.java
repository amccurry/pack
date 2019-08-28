package pack.iscsi.partitioned.storagemanager;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;

import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.storagemanager.cache.BlockCacheLoader;
import pack.iscsi.partitioned.storagemanager.cache.BlockRemovalListener;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;

public class BlockStorageModuleFactory implements StorageModuleFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModuleFactory.class);

  private final LoadingCache<BlockKey, Block> _cache;
  private final BlockStore _blockStore;
  private final BlockWriteAheadLog _writeAheadLog;
  private final ExternalBlockIOFactory _externalBlockStoreFactory;
  private final File _blockDataDir;

  public BlockStorageModuleFactory(BlockStorageModuleFactoryConfig config) {
    _blockDataDir = config.getBlockDataDir();
    _blockStore = config.getBlockStore();
    _writeAheadLog = config.getWriteAheadLog();
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();
    CacheLoader<BlockKey, Block> loader = new BlockCacheLoader(_blockStore, _writeAheadLog, _blockDataDir,
        _externalBlockStoreFactory);

    RemovalListener<BlockKey, Block> removalListener = new BlockRemovalListener(_externalBlockStoreFactory);

    Weigher<BlockKey, Block> weigher = (key, value) -> value.getSize();

    _cache = Caffeine.newBuilder()
                     .removalListener(removalListener)
                     .weigher(weigher)
                     .maximumWeight(config.getMaxCacheSizeInBytes())
                     .build(loader);
  }

  @Override
  public List<String> getStorageModuleNames() throws IOException {
    return _blockStore.getVolumeNames();
  }

  @Override
  public StorageModule getStorageModule(String name) throws IOException {
    long volumeId = _blockStore.getVolumeId(name);
    int blockSize = _blockStore.getBlockSize(volumeId);
    long lengthInBytes = _blockStore.getLengthInBytes(volumeId);
    LOGGER.info("open storage module for {}({})", name, volumeId);
    return new BlockStorageModule(_cache, volumeId, blockSize, lengthInBytes, _externalBlockStoreFactory);
  }

}
