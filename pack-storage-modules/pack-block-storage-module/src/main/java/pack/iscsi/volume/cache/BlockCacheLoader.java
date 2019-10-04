package pack.iscsi.volume.cache;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;

import io.opentracing.Scope;
import pack.iscsi.block.LocalBlock;
import pack.iscsi.block.LocalBlockConfig;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.util.Utils;
import pack.iscsi.volume.cache.wal.BlockWriteAheadLogRecovery;
import pack.iscsi.volume.cache.wal.BlockWriteAheadLogRecovery.BlockWriteAheadLogRecoveryConfig;
import pack.util.TracerUtil;

public class BlockCacheLoader implements CacheLoader<BlockKey, Block> {

  private static Logger LOGGER = LoggerFactory.getLogger(BlockCacheLoader.class);

  private final BlockGenerationStore _blockGenerationStore;
  private final BlockWriteAheadLog _writeAheadLog;
  private final BlockIOFactory _externalBlockStoreFactory;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;
  private final BlockRemovalListener _removalListener;
  private final long _volumeId;
  private final int _blockSize;
  private final BlockStateStore _blockStateStore;
  private final RandomAccessIO _randomAccessIO;

  public BlockCacheLoader(BlockCacheLoaderConfig config) {
    _blockStateStore = config.getBlockStateStore();
    _volumeId = config.getVolumeId();
    _blockSize = config.getBlockSize();
    _blockGenerationStore = config.getBlockGenerationStore();
    _writeAheadLog = config.getWriteAheadLog();
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();
    _syncTimeAfterIdle = config.getSyncTimeAfterIdle();
    _syncTimeAfterIdleTimeUnit = config.getSyncTimeAfterIdleTimeUnit();
    _removalListener = config.getRemovalListener();
    _randomAccessIO = config.getRandomAccessIO();
  }

  @Override
  public Block load(BlockKey key) throws Exception {
    try (Scope blockLoader = TracerUtil.trace(BlockCacheLoader.class, "block loader")) {
      Block stolenBlock = _removalListener.stealBlock(key);
      if (stolenBlock != null) {
        return stolenBlock;
      }
      LocalBlockConfig config = LocalBlockConfig.builder()
                                                .randomAccessIO(_randomAccessIO)
                                                .blockStateStore(_blockStateStore)
                                                .volumeId(_volumeId)
                                                .blockSize(_blockSize)
                                                .blockId(key.getBlockId())
                                                .blockGenerationStore(_blockGenerationStore)
                                                .wal(_writeAheadLog)
                                                .syncTimeAfterIdle(_syncTimeAfterIdle)
                                                .syncTimeAfterIdleTimeUnit(_syncTimeAfterIdleTimeUnit)
                                                .build();
      LocalBlock localBlock;
      try (Scope scope = TracerUtil.trace(BlockCacheLoader.class, "create local block")) {
        localBlock = new LocalBlock(config);
      }
      if (localBlock.getLastStoredGeneration() != Block.MISSING_BLOCK_GENERATION) {
        pullBlockFromExternalStore(localBlock);
      } else {
        localBlock.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));
      }
      recoverFromWal(localBlock);
      return localBlock;
    }
  }

  private void pullBlockFromExternalStore(LocalBlock localBlock) {
    try (Scope externalRead = TracerUtil.trace(BlockCacheLoader.class, "block external read")) {
      Utils.runUntilSuccess(LOGGER, () -> {
        localBlock.execIO(_externalBlockStoreFactory.getBlockReader());
        return null;
      });
    }
  }

  private void recoverFromWal(LocalBlock localBlock) {
    try (Scope externalRead = TracerUtil.trace(BlockCacheLoader.class, "block recover")) {
      Utils.runUntilSuccess(LOGGER, () -> {
        // recover if needed
        BlockWriteAheadLogRecoveryConfig config = BlockWriteAheadLogRecoveryConfig.builder()
                                                                                  .blockWriteAheadLog(_writeAheadLog)
                                                                                  .build();
        BlockWriteAheadLogRecovery recovery = new BlockWriteAheadLogRecovery(config);
        localBlock.execIO(recovery);
        return null;
      });
    }
  }

}
