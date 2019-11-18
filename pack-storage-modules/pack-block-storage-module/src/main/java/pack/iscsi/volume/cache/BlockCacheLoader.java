package pack.iscsi.volume.cache;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;

import io.opentracing.Scope;
import pack.iscsi.block.LocalBlock;
import pack.iscsi.block.LocalBlockConfig;
import pack.iscsi.concurrent.ConcurrentUtils;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.block.BlockStateStore;
import pack.util.tracer.TracerUtil;

public class BlockCacheLoader implements CacheLoader<BlockKey, Block> {

  private static Logger LOGGER = LoggerFactory.getLogger(BlockCacheLoader.class);

  private final BlockGenerationStore _blockGenerationStore;
  private final BlockIOFactory _externalBlockStoreFactory;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;
  private final BlockRemovalListener _removalListener;
  private final long _volumeId;
  private final int _blockSize;
  private final BlockStateStore _blockStateStore;
  private final LocalFileCacheFactory _localFileCache;

  public BlockCacheLoader(BlockCacheLoaderConfig config) {
    _blockStateStore = config.getBlockStateStore();
    _volumeId = config.getVolumeId();
    _blockSize = config.getBlockSize();
    _blockGenerationStore = config.getBlockGenerationStore();
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();
    _syncTimeAfterIdle = config.getSyncTimeAfterIdle();
    _syncTimeAfterIdleTimeUnit = config.getSyncTimeAfterIdleTimeUnit();
    _removalListener = config.getRemovalListener();
    _localFileCache = config.getLocalFileCache();
  }

  @Override
  public Block load(BlockKey key) throws Exception {
    try (Scope blockLoader = TracerUtil.trace(BlockCacheLoader.class, "block loader")) {
      Block stolenBlock = _removalListener.stealBlock(key);
      if (stolenBlock != null) {
        LOGGER.info("volumeId {} blockId {} stolen from cache", _volumeId, stolenBlock.getBlockId());
        return stolenBlock;
      }
      RandomAccessIO randomAccessIO = _localFileCache.getRandomAccessIO(_volumeId, key.getBlockId());
      LocalBlockConfig config = LocalBlockConfig.builder()
                                                .randomAccessIO(randomAccessIO)
                                                .blockStateStore(_blockStateStore)
                                                .volumeId(_volumeId)
                                                .blockSize(_blockSize)
                                                .blockId(key.getBlockId())
                                                .blockGenerationStore(_blockGenerationStore)
                                                .syncTimeAfterIdle(_syncTimeAfterIdle)
                                                .syncTimeAfterIdleTimeUnit(_syncTimeAfterIdleTimeUnit)
                                                .build();
      LocalBlock localBlock;
      try (Scope scope = TracerUtil.trace(BlockCacheLoader.class, "create local block")) {
        localBlock = new LocalBlock(config);
      }
      long onDiskGeneration = localBlock.getOnDiskGeneration();
      long lastStoredGeneration = localBlock.getLastStoredGeneration();
      if (onDiskGeneration == lastStoredGeneration) {
        if (onDiskGeneration == Block.MISSING_BLOCK_GENERATION) {
          // Initialize new block
          localBlock.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));
        }
      } else if (onDiskGeneration < lastStoredGeneration) {
        // On disk behind last stored block, pull a newer version
        try (Scope scope = TracerUtil.trace(BlockCacheLoader.class, "pull remote block")) {
          pullBlockFromExternalStore(localBlock);
        }
      }
      return localBlock;
    }
  }

  private void pullBlockFromExternalStore(LocalBlock localBlock) {
    try (Scope externalRead = TracerUtil.trace(BlockCacheLoader.class, "block external read")) {
      ConcurrentUtils.runUntilSuccess(LOGGER, () -> {
        localBlock.execIO(_externalBlockStoreFactory.getBlockReader());
        return null;
      });
    }
  }

}
