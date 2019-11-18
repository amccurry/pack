package pack.iscsi.volume;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.concurrent.ConcurrentUtils;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;
import pack.iscsi.spi.VolumeListener;
import pack.iscsi.spi.block.BlockCacheMetadataStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.iscsi.util.Utils;

public class BlockStorageModuleFactory implements StorageModuleFactory, Closeable, VolumeListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModuleFactory.class);

  private final BlockGenerationStore _blockGenerationStore;
  private final File[] _blockDataDirs;
  private final PackVolumeStore _packVolumeStore;
  private final BlockIOFactory _externalBlockStoreFactory;
  private final MetricsFactory _metricsFactory;
  private final long _maxCacheSizeInBytes;
  private final ConcurrentMap<String, BlockStorageModule> _blockStorageModules = new ConcurrentHashMap<>();
  private final Object _lock = new Object();
  private final BlockStateStore _blockStateStore;
  private final BlockCacheMetadataStore _blockCacheMetadataStore;
  private final Timer _gcTimer;
  private final ExecutorService _gcExecutor;
  private final int _defaultSyncExecutorThreadCount;
  private final int _defaultReadAheadExecutorThreadCount;
  private final int _defaultReadAheadBlockLimit;
  private final long _defaultSyncTimeAfterIdle;
  private final TimeUnit _defaultSyncTimeAfterIdleTimeUnit;

  public BlockStorageModuleFactory(BlockStorageModuleFactoryConfig config) {
    _defaultSyncExecutorThreadCount = config.getDefaultSyncExecutorThreadCount();
    _defaultReadAheadExecutorThreadCount = config.getDefaultReadAheadExecutorThreadCount();
    _defaultReadAheadBlockLimit = config.getDefaultReadAheadBlockLimit();
    _defaultSyncTimeAfterIdle = config.getDefaultSyncTimeAfterIdle();
    _defaultSyncTimeAfterIdleTimeUnit = config.getDefaultSyncTimeAfterIdleTimeUnit();

    _blockCacheMetadataStore = config.getBlockCacheMetadataStore();
    _blockStateStore = config.getBlockStateStore();
    _packVolumeStore = config.getPackVolumeStore();
    _blockDataDirs = config.getBlockDataDirs();
    _blockGenerationStore = config.getBlockStore();
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();

    _metricsFactory = config.getMetricsFactory();
    _maxCacheSizeInBytes = config.getMaxCacheSizeInBytes();
    _gcExecutor = ConcurrentUtils.executor("gc", config.getGcExecutorThreadCount());
    _gcTimer = new Timer("gc-driver", true);
    long gcPeriod = config.getGcDriverTimeUnit()
                          .toMillis(config.getGcDriverTime());
    _gcTimer.schedule(getGcTask(), gcPeriod, gcPeriod);
    _packVolumeStore.register(this);
  }

  @Override
  public List<String> getStorageModuleNames() throws IOException {
    return _packVolumeStore.getAttachedVolumes();
  }

  @Override
  public StorageModule getStorageModule(String name) throws IOException {
    synchronized (_lock) {

      PackVolumeMetadata volumeMetadata = _packVolumeStore.getVolumeMetadata(name);
      if (!_packVolumeStore.isAttached(name)) {
        throw new IOException("Volume " + name + " is not attached to this host.");
      }

      long syncTimeAfterIdle = getValue(volumeMetadata.getSyncTimeAfterIdle(), _defaultSyncTimeAfterIdle);
      TimeUnit syncTimeAfterIdleTimeUnit = getValue(volumeMetadata.getSyncTimeAfterIdleTimeUnit(),
          _defaultSyncTimeAfterIdleTimeUnit);
      int syncExecutorThreadCount = getValue(volumeMetadata.getSyncExecutorThreadCount(),
          _defaultSyncExecutorThreadCount);
      int readAheadExecutorThreadCount = getValue(volumeMetadata.getReadAheadExecutorThreadCount(),
          _defaultReadAheadExecutorThreadCount);
      int readAheadBlockLimit = getValue(volumeMetadata.getReadAheadBlockLimit(), _defaultReadAheadBlockLimit);

      BlockStorageModule storageModule = _blockStorageModules.get(name);
      if (storageModule != null) {
        return referenceCounter(name, volumeMetadata, storageModule);
      }

      long volumeId = volumeMetadata.getVolumeId();
      int blockSize = volumeMetadata.getBlockSizeInBytes();
      long lengthInBytes = volumeMetadata.getLengthInBytes();
      long blockCount = Utils.getBlockCount(lengthInBytes, blockSize);

      BlockStorageModuleConfig config = BlockStorageModuleConfig.builder()
                                                                .readOnly(volumeMetadata.isReadOnly())
                                                                .blockCacheMetadataStore(_blockCacheMetadataStore)
                                                                .blockDataDirs(_blockDataDirs)
                                                                .blockGenerationStore(_blockGenerationStore)
                                                                .blockStateStore(_blockStateStore)
                                                                .blockSize(blockSize)
                                                                .externalBlockStoreFactory(_externalBlockStoreFactory)
                                                                .blockCount(blockCount)
                                                                .volumeName(name)
                                                                .volumeId(volumeId)
                                                                .syncTimeAfterIdle(syncTimeAfterIdle)
                                                                .syncTimeAfterIdleTimeUnit(syncTimeAfterIdleTimeUnit)
                                                                .metricsFactory(_metricsFactory)
                                                                .maxCacheSizeInBytes(getMaxCacheSizeInBytes())
                                                                .syncExecutorThreadCount(syncExecutorThreadCount)
                                                                .readAheadExecutorThreadCount(
                                                                    readAheadExecutorThreadCount)
                                                                .readAheadBlockLimit(readAheadBlockLimit)
                                                                .build();
      LOGGER.info("open storage module for {}({})", name, volumeId);
      storageModule = new BlockStorageModule(config);
      _blockStorageModules.put(name, storageModule);
      return new TimerStorageModule(referenceCounter(name, volumeMetadata, storageModule));
    }
  }

  private static <T> T getValue(T overrideValue, T defaultValue) {
    if (overrideValue != null) {
      return overrideValue;
    }
    return defaultValue;
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("starting close of storage module factory");
    _gcTimer.purge();
    _gcTimer.cancel();
    IOUtils.close(LOGGER, _gcExecutor);
    IOUtils.close(LOGGER, _blockStorageModules.values());
  }

  private StorageModule referenceCounter(String name, PackVolumeMetadata volumeMetadata,
      BlockStorageModule storageModule) {
    Thread thread = Thread.currentThread();
    String connectionInfo = thread.getName();
    thread.setName(volumeMetadata.getVolumeId() + connectionInfo);
    storageModule.incrementRef();
    return new DelegateStorageModule(storageModule) {
      @Override
      public void close() throws IOException {
        synchronized (_lock) {
          storageModule.decrementRef();
          LOGGER.info("reference count {}", storageModule.getRefCount());
          if (storageModule.getRefCount() == 0) {
            storageModule.close();
            _blockStorageModules.remove(name);
          }
        }
      }
    };
  }

  private long getMaxCacheSizeInBytes() {
    return _maxCacheSizeInBytes;
  }

  @Override
  public void lengthChange(PackVolumeMetadata packVolumeMetadata) throws IOException {
    getModule(packVolumeMetadata).setLengthInBytes(packVolumeMetadata.getLengthInBytes());
  }

  @Override
  public void sync(PackVolumeMetadata packVolumeMetadata, boolean blocking, boolean onlyIfIdleWrites)
      throws IOException {
    getModule(packVolumeMetadata).sync(blocking, onlyIfIdleWrites);
  }

  @Override
  public Map<BlockKey, Long> createSnapshot(PackVolumeMetadata packVolumeMetadata) throws IOException {
    return getModule(packVolumeMetadata).createSnapshot();
  }

  @Override
  public boolean isInUse(PackVolumeMetadata metadata) throws IOException {
    return _blockStorageModules.containsKey(metadata.getName());
  }

  @Override
  public boolean hasVolume(PackVolumeMetadata metadata) throws IOException {
    String name = metadata.getName();
    return _blockStorageModules.containsKey(name);
  }

  private BlockStorageModule getModule(PackVolumeMetadata packVolumeMetadata) throws IOException {
    String name = packVolumeMetadata.getName();
    BlockStorageModule module = _blockStorageModules.get(name);
    if (module == null) {
      throw new IOException("Storage module " + name + " not found");
    }
    return module;
  }

  private TimerTask getGcTask() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          runGc();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
      }
    };
  }

  private void runGc() throws IOException {
    List<String> volumes = _packVolumeStore.getAttachedVolumes();
    List<Future<Void>> futures = new ArrayList<>();
    for (String volume : volumes) {
      futures.add(_gcExecutor.submit(() -> {
        _packVolumeStore.gc(volume);
        return null;
      }));
    }
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        LOGGER.error(e.getMessage(), e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        LOGGER.error(cause.getMessage(), cause);
      }
    }
  }
}
