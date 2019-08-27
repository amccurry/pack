package pack.iscsi.partitioned.storagemanager;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;

import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.block.LocalBlock;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;

public class BlockStorageModuleFactory implements StorageModuleFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModuleFactory.class);

  private final LoadingCache<BlockKey, Block> _cache;
  private final BlockStore _blockStore;
  private final BlockWriteAheadLog _writeAheadLog;

  private final File _blockDataDir;
  private final ExternalBlockIOFactory _externalBlockStoreFactory;

  public BlockStorageModuleFactory(BlockStorageModuleFactoryConfig config) {
    _blockDataDir = config.getBlockDataDir();
    _blockStore = config.getBlockStore();
    _writeAheadLog = config.getWriteAheadLog();
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();

    CacheLoader<BlockKey, Block> loader = new CacheLoader<BlockKey, Block>() {

      @Override
      public Block load(BlockKey key) throws Exception {
        long volumeId = key.getVolumeId();
        int blockSize = _blockStore.getBlockSize(volumeId);
        LocalBlock block = new LocalBlock(_blockDataDir, volumeId, key.getBlockId(), blockSize, _blockStore,
            _writeAheadLog);
        while (true) {
          try {
            block.execIO(_externalBlockStoreFactory.getBlockReader());
            break;
          } catch (IOException e) {
            LOGGER.error("Unknown error", e);
            sleep();
          }
        }
        return block;
      }

    };

    RemovalListener<BlockKey, Block> removalListener = new RemovalListener<BlockKey, Block>() {
      @Override
      public void onRemoval(BlockKey key, Block value, RemovalCause cause) {
        while (true) {
          try {
            value.execIO(_externalBlockStoreFactory.getBlockWriter());
            break;
          } catch (IOException e) {
            LOGGER.error("Unknown error", e);
            sleep();
          }
        }
        // add to async close and cleanup
        try {
          value.close();
        } catch (IOException e) {
          LOGGER.error("Unknown error", e);
        }
        try {
          value.cleanUp();
        } catch (IOException e) {
          LOGGER.error("Unknown error", e);
        }
      }
    };

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
    return new BlockStorageModule(_cache, volumeId, blockSize, lengthInBytes);
  }

  private void sleep() {
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(3));
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }
}
