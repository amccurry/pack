package pack.iscsi.volume;

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

import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.MetricsFactory;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockKey;
import pack.iscsi.spi.volume.VolumeMetadata;
import pack.iscsi.spi.volume.VolumeStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.util.Utils;
import pack.iscsi.volume.cache.BlockCacheLoader;
import pack.iscsi.volume.cache.BlockCacheLoaderConfig;
import pack.iscsi.volume.cache.BlockRemovalListener;
import pack.iscsi.volume.cache.BlockRemovalListenerConfig;

public class BlockStorageModuleFactory implements StorageModuleFactory, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModuleFactory.class);

  private static final String SYNC = "sync";

  private final LoadingCache<BlockKey, Block> _cache;
  private final BlockGenerationStore _blockStore;
  private final VolumeStore _volumeStore;
  private final BlockWriteAheadLog _writeAheadLog;
  private final BlockIOFactory _externalBlockStoreFactory;
  private final File _blockDataDir;
  private final ExecutorService _syncExecutor;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;
  private final MetricsFactory _metricsFactory;

  public BlockStorageModuleFactory(BlockStorageModuleFactoryConfig config) {
    _volumeStore = config.getVolumeStore();
    _blockDataDir = config.getBlockDataDir();
    _blockStore = config.getBlockStore();
    _writeAheadLog = config.getWriteAheadLog();
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();
    _syncTimeAfterIdle = config.getSyncTimeAfterIdle();
    _syncTimeAfterIdleTimeUnit = config.getSyncTimeAfterIdleTimeUnit();
    _syncExecutor = Utils.executor(SYNC, config.getSyncThreads());
    _metricsFactory = config.getMetricsFactory();

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
    return _volumeStore.getVolumeNames();
  }

  @Override
  public StorageModule getStorageModule(String name) throws IOException {
    VolumeMetadata volumeMetadata = _volumeStore.getVolumeMetadata(name);
    long volumeId = volumeMetadata.getVolumeId();
    int blockSize = volumeMetadata.getBlockSize();
    long lengthInBytes = volumeMetadata.getLengthInBytes();
    BlockStorageModuleConfig config = BlockStorageModuleConfig.builder()
                                                              .blockSize(blockSize)
                                                              .externalBlockStoreFactory(_externalBlockStoreFactory)
                                                              .globalCache(_cache)
                                                              .lengthInBytes(lengthInBytes)
                                                              .volumeName(name)
                                                              .volumeId(volumeId)
                                                              .syncTimeAfterIdle(_syncTimeAfterIdle)
                                                              .syncTimeAfterIdleTimeUnit(_syncTimeAfterIdleTimeUnit)
                                                              .syncExecutor(_syncExecutor)
                                                              .metricsFactory(_metricsFactory)
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
                                                      .volumeStore(_volumeStore)
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
