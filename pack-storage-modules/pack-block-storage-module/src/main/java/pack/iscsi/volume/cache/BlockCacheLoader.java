package pack.iscsi.volume.cache;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;

import io.opencensus.common.Scope;
import pack.iscsi.block.LocalBlock;
import pack.iscsi.block.LocalBlockConfig;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockKey;
import pack.iscsi.spi.volume.VolumeMetadata;
import pack.iscsi.spi.volume.VolumeStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.util.Utils;
import pack.iscsi.volume.cache.wal.BlockWriteAheadLogRecovery;
import pack.iscsi.volume.cache.wal.BlockWriteAheadLogRecovery.BlockWriteAheadLogRecoveryConfig;
import pack.util.TracerUtil;

public class BlockCacheLoader implements CacheLoader<BlockKey, Block> {

  private static Logger LOGGER = LoggerFactory.getLogger(BlockCacheLoader.class);

  private final VolumeStore _volumeStore;
  private final BlockGenerationStore _blockStore;
  private final BlockWriteAheadLog _writeAheadLog;
  private final File _blockDataDir;
  private final BlockIOFactory _externalBlockStoreFactory;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;
  private final BlockRemovalListener _removalListener;

  public BlockCacheLoader(BlockCacheLoaderConfig config) {
    _volumeStore = config.getVolumeStore();
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
    try (Scope blockLoader = TracerUtil.trace("block loader")) {
      Block stolenBlock = _removalListener.stealBlock(key);
      if (stolenBlock != null) {
        return stolenBlock;
      }
      VolumeMetadata volumeMetadata = _volumeStore.getVolumeMetadata(key.getVolumeId());
      LocalBlockConfig config = LocalBlockConfig.builder()
                                                .blockDataDir(_blockDataDir)
                                                .volumeMetadata(volumeMetadata)
                                                .blockId(key.getBlockId())
                                                .blockStore(_blockStore)
                                                .wal(_writeAheadLog)
                                                .syncTimeAfterIdle(_syncTimeAfterIdle)
                                                .syncTimeAfterIdleTimeUnit(_syncTimeAfterIdleTimeUnit)
                                                .build();

      LocalBlock localBlock = new LocalBlock(config);
      try (Scope externalRead = TracerUtil.trace("block external read")) {
        Utils.runUntilSuccess(LOGGER, () -> {
          localBlock.execIO(_externalBlockStoreFactory.getBlockReader());
          return null;
        });
      }
      try (Scope externalRead = TracerUtil.trace("block recover")) {
        Utils.runUntilSuccess(LOGGER, () -> {
          // recover if needed
          BlockWriteAheadLogRecovery recovery = new BlockWriteAheadLogRecovery(
              BlockWriteAheadLogRecoveryConfig.builder()
                                              .blockWriteAheadLog(_writeAheadLog)
                                              .build());
          localBlock.execIO(recovery);
          return null;
        });
      }
      return localBlock;
    }
  }

}
