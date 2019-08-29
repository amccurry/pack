package pack.iscsi.partitioned.storagemanager;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;

import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.storagemanager.cache.BlockCacheLoader;
import pack.iscsi.partitioned.storagemanager.cache.BlockCacheLoaderConfig;
import pack.iscsi.partitioned.storagemanager.cache.BlockRemovalListener;
import pack.iscsi.partitioned.storagemanager.cache.BlockRemovalListenerConfig;
import pack.iscsi.partitioned.util.Utils;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;
import pack.util.IOUtils;

public class BlockStorageModuleFactory implements StorageModuleFactory, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModuleFactory.class);

  private static final String SYNC = "sync";

  private final LoadingCache<BlockKey, Block> _cache;
  private final BlockStore _blockStore;
  private final BlockWriteAheadLog _writeAheadLog;
  private final ExternalBlockIOFactory _externalBlockStoreFactory;
  private final File _blockDataDir;
  private final ExecutorService _syncExecutor;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;

  public BlockStorageModuleFactory(BlockStorageModuleFactoryConfig config) {
    _blockDataDir = config.getBlockDataDir();
    _blockStore = config.getBlockStore();
    _writeAheadLog = config.getWriteAheadLog();
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();
    _syncTimeAfterIdle = config.getSyncTimeAfterIdle();
    _syncTimeAfterIdleTimeUnit = config.getSyncTimeAfterIdleTimeUnit();
    _syncExecutor = Utils.executor(SYNC, config.getSyncThreads());

    BlockRemovalListener removalListener = getRemovalListener();

    BlockCacheLoader loader = getCacheLoader(removalListener);

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
    BlockStorageModuleConfig config = BlockStorageModuleConfig.builder()
                                                              .blockSize(blockSize)
                                                              .externalBlockStoreFactory(_externalBlockStoreFactory)
                                                              .globalCache(_cache)
                                                              .lengthInBytes(lengthInBytes)
                                                              .volumeId(volumeId)
                                                              .syncTimeAfterIdle(_syncTimeAfterIdle)
                                                              .syncTimeAfterIdleTimeUnit(_syncTimeAfterIdleTimeUnit)
                                                              .syncExecutor(_syncExecutor)
                                                              .build();
    LOGGER.info("open storage module for {}({})", name, volumeId);
    return new BlockStorageModule(config);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, _syncExecutor);
  }

  private BlockRemovalListener getRemovalListener() {
    return new BlockRemovalListener(BlockRemovalListenerConfig.builder()
                                                              .externalBlockStoreFactory(_externalBlockStoreFactory)
                                                              .build());
  }

  private BlockCacheLoader getCacheLoader(BlockRemovalListener removalListener) {
    return new BlockCacheLoader(BlockCacheLoaderConfig.builder()
                                                      .blockDataDir(_blockDataDir)
                                                      .blockStore(_blockStore)
                                                      .externalBlockStoreFactory(_externalBlockStoreFactory)
                                                      .syncTimeAfterIdle(_syncTimeAfterIdle)
                                                      .syncTimeAfterIdleTimeUnit(_syncTimeAfterIdleTimeUnit)
                                                      .writeAheadLog(_writeAheadLog)
                                                      .removalListener(removalListener)
                                                      .build());
  }
}
