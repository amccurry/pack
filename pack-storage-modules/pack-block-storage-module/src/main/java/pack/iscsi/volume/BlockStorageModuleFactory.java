package pack.iscsi.volume;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.util.Utils;

public class BlockStorageModuleFactory implements StorageModuleFactory, Closeable, VolumeListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModuleFactory.class);

  private final BlockGenerationStore _blockGenerationStore;
  private final BlockWriteAheadLog _writeAheadLog;
  private final File _blockDataDir;
  private final PackVolumeStore _packVolumeStore;
  private final BlockIOFactory _externalBlockStoreFactory;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;
  private final MetricsFactory _metricsFactory;
  private final long _maxCacheSizeInBytes;
  private final ConcurrentMap<String, BlockStorageModule> _blockStorageModules = new ConcurrentHashMap<>();
  private final Object _lock = new Object();
  private final BlockStateStore _blockStateStore;
  private final BlockCacheMetadataStore _blockCacheMetadataStore;

  public BlockStorageModuleFactory(BlockStorageModuleFactoryConfig config) {
    _blockCacheMetadataStore = config.getBlockCacheMetadataStore();
    _blockStateStore = config.getBlockStateStore();
    _packVolumeStore = config.getPackVolumeStore();
    _blockDataDir = config.getBlockDataDir();
    _blockGenerationStore = config.getBlockStore();
    _writeAheadLog = config.getWriteAheadLog();
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();
    _syncTimeAfterIdle = config.getSyncTimeAfterIdle();
    _syncTimeAfterIdleTimeUnit = config.getSyncTimeAfterIdleTimeUnit();
    _metricsFactory = config.getMetricsFactory();
    _maxCacheSizeInBytes = config.getMaxCacheSizeInBytes();
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

      BlockStorageModule storageModule = _blockStorageModules.get(name);
      if (storageModule != null) {
        return referenceCounter(name, volumeMetadata, storageModule);
      }

      long volumeId = volumeMetadata.getVolumeId();
      int blockSize = volumeMetadata.getBlockSizeInBytes();
      long lengthInBytes = volumeMetadata.getLengthInBytes();
      long blockCount = Utils.getBlockCount(lengthInBytes, blockSize);

      BlockStorageModuleConfig config = BlockStorageModuleConfig.builder()
                                                                .blockCacheMetadataStore(_blockCacheMetadataStore)
                                                                .blockDataDir(_blockDataDir)
                                                                .blockGenerationStore(_blockGenerationStore)
                                                                .blockStateStore(_blockStateStore)
                                                                .blockSize(blockSize)
                                                                .externalBlockStoreFactory(_externalBlockStoreFactory)
                                                                .blockCount(blockCount)
                                                                .volumeName(name)
                                                                .volumeId(volumeId)
                                                                .syncTimeAfterIdle(_syncTimeAfterIdle)
                                                                .syncTimeAfterIdleTimeUnit(_syncTimeAfterIdleTimeUnit)
                                                                .metricsFactory(_metricsFactory)
                                                                .writeAheadLog(_writeAheadLog)
                                                                .maxCacheSizeInBytes(getMaxCacheSizeInBytes())
                                                                .build();
      LOGGER.info("open storage module for {}({})", name, volumeId);
      storageModule = new BlockStorageModule(config);
      _blockStorageModules.put(name, storageModule);
      return referenceCounter(name, volumeMetadata, storageModule);
    }
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("starting close of storage module factory");
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
}
