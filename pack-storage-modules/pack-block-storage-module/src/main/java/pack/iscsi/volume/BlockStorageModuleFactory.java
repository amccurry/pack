package pack.iscsi.volume;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;
import pack.iscsi.spi.VolumeLengthListener;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.util.Utils;

public class BlockStorageModuleFactory implements StorageModuleFactory, Closeable, VolumeLengthListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModuleFactory.class);

  private static final String SYNC = "sync";

  private final BlockGenerationStore _blockGenerationStore;
  private final BlockWriteAheadLog _writeAheadLog;
  private final File _blockDataDir;
  private final PackVolumeStore _packVolumeStore;
  private final BlockIOFactory _externalBlockStoreFactory;
  private final ExecutorService _syncExecutor;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;
  private final MetricsFactory _metricsFactory;
  private final long _maxCacheSizeInBytes;
  private final ConcurrentMap<String, BlockStorageModule> _blockStorageModules = new ConcurrentHashMap<>();
  private final Object _lock = new Object();

  public BlockStorageModuleFactory(BlockStorageModuleFactoryConfig config) {
    _packVolumeStore = config.getPackVolumeStore();
    _blockDataDir = config.getBlockDataDir();
    _blockGenerationStore = config.getBlockStore();
    _writeAheadLog = config.getWriteAheadLog();
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();
    _syncTimeAfterIdle = config.getSyncTimeAfterIdle();
    _syncTimeAfterIdleTimeUnit = config.getSyncTimeAfterIdleTimeUnit();
    _syncExecutor = Utils.executor(SYNC, config.getSyncThreads());
    _metricsFactory = config.getMetricsFactory();
    _maxCacheSizeInBytes = config.getMaxCacheSizeInBytes();
    _packVolumeStore.register(this);
  }

  @Override
  public List<String> getStorageModuleNames() throws IOException {
    return _packVolumeStore.getAssignedVolumes();
  }

  @Override
  public StorageModule getStorageModule(String name) throws IOException {
    synchronized (_lock) {

      PackVolumeMetadata volumeMetadata = _packVolumeStore.getVolumeMetadata(name);
      if (!_packVolumeStore.isAssigned(name)) {
        throw new IOException("Volume " + name + " is not assigned to this host.");
      }

      BlockStorageModule storageModule = _blockStorageModules.get(name);
      if (storageModule != null) {
        return referenceCounter(name, volumeMetadata, storageModule);
      }

      long volumeId = volumeMetadata.getVolumeId();
      int blockSize = volumeMetadata.getBlockSizeInBytes();
      long lengthInBytes = volumeMetadata.getLengthInBytes();
      BlockStorageModuleConfig config = BlockStorageModuleConfig.builder()
                                                                .blockDataDir(_blockDataDir)
                                                                .blockGenerationStore(_blockGenerationStore)
                                                                .blockSize(blockSize)
                                                                .externalBlockStoreFactory(_externalBlockStoreFactory)
                                                                .lengthInBytes(lengthInBytes)
                                                                .volumeName(name)
                                                                .volumeId(volumeId)
                                                                .syncTimeAfterIdle(_syncTimeAfterIdle)
                                                                .syncTimeAfterIdleTimeUnit(_syncTimeAfterIdleTimeUnit)
                                                                .syncExecutor(_syncExecutor)
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
    IOUtils.close(LOGGER, _syncExecutor);
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
    String name = packVolumeMetadata.getName();
    BlockStorageModule blockStorageModule = _blockStorageModules.get(name);
    if (blockStorageModule != null) {
      blockStorageModule.setLengthInBytes(packVolumeMetadata.getLengthInBytes());
    }
  }

}
