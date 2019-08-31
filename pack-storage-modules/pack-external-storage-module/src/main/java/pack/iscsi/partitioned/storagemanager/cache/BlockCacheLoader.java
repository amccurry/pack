package pack.iscsi.partitioned.storagemanager.cache;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;

import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.block.LocalBlock;
import pack.iscsi.partitioned.block.LocalBlockConfig;
import pack.iscsi.partitioned.storagemanager.BlockKey;
import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
import pack.iscsi.partitioned.storagemanager.BlockIOFactory;
import pack.iscsi.partitioned.util.Utils;

public class BlockCacheLoader implements CacheLoader<BlockKey, Block> {

  private static Logger LOGGER = LoggerFactory.getLogger(BlockCacheLoader.class);

  private final BlockStore _blockStore;
  private final BlockWriteAheadLog _writeAheadLog;
  private final File _blockDataDir;
  private final BlockIOFactory _externalBlockStoreFactory;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;
  private final BlockRemovalListener _removalListener;

  public BlockCacheLoader(BlockCacheLoaderConfig config) {
    _blockDataDir = config.getBlockDataDir();
    _blockStore = config.getBlockStore();
    _writeAheadLog = config.getWriteAheadLog();
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();
    _syncTimeAfterIdle = config.getSyncTimeAfterIdle();
    _syncTimeAfterIdleTimeUnit = config.getSyncTimeAfterIdleTimeUnit();
    _removalListener = config.getRemovalListener();
  }

  @Override
  public Block load(BlockKey key) throws Exception {
    Block stolenBlock = _removalListener.stealBlock(key);
    if (stolenBlock != null) {
      return stolenBlock;
    }
    long volumeId = key.getVolumeId();
    int blockSize = _blockStore.getBlockSize(volumeId);

    LocalBlockConfig config = LocalBlockConfig.builder()
                                              .blockDataDir(_blockDataDir)
                                              .volumeId(volumeId)
                                              .blockId(key.getBlockId())
                                              .blockSize(blockSize)
                                              .blockStore(_blockStore)
                                              .wal(_writeAheadLog)
                                              .syncTimeAfterIdle(_syncTimeAfterIdle)
                                              .syncTimeAfterIdleTimeUnit(_syncTimeAfterIdleTimeUnit)
                                              .build();

    LocalBlock localBlock = new LocalBlock(config);
    Utils.runUntilSuccess(LOGGER, () -> {
      localBlock.execIO(_externalBlockStoreFactory.getBlockReader());
      return null;
    });
    Utils.runUntilSuccess(LOGGER, () -> {
      // recover if needed
      localBlock.execIO(_writeAheadLog.getWriteAheadLogReader());
      return null;
    });
    return localBlock;
  }

}
