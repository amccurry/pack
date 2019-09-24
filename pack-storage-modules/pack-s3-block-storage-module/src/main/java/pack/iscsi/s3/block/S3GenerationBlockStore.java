package pack.iscsi.s3.block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Splitter;

import consistent.s3.ConsistentAmazonS3;
import io.opencensus.common.Scope;
import lombok.Builder;
import lombok.Value;
import pack.iscsi.s3.util.S3Utils;
import pack.iscsi.s3.util.S3Utils.ListResultProcessor;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockKey;
import pack.util.ExecutorUtil;
import pack.util.TracerUtil;

public class S3GenerationBlockStore implements BlockGenerationStore {

  @Value
  @Builder
  public static class S3GenerationBlockStoreConfig {

    ConsistentAmazonS3 consistentAmazonS3;
    String bucket;
    String objectPrefix;

  }

  private static final Logger LOGGER = LoggerFactory.getLogger(S3GenerationBlockStore.class);

  private static final Splitter KEY_SPLITTER = Splitter.on('/');
  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;
  private final LoadingCache<BlockKey, Long> _cache;

  public S3GenerationBlockStore(S3GenerationBlockStoreConfig config) {
    _consistentAmazonS3 = config.getConsistentAmazonS3();
    _bucket = config.getBucket();
    _objectPrefix = config.getObjectPrefix();

    CacheLoader<BlockKey, Long> loader = key -> {
      String blockKey = S3Utils.getBlockKeyPrefix(_objectPrefix, key.getVolumeId(), key.getBlockId());
      LOGGER.info("blockkey {} scan key from {}", key, blockKey);
      List<String> keys;
      try (Scope scope = TracerUtil.trace(S3GenerationBlockStore.class, "s3 list objects")) {
        keys = S3Utils.listObjects(_consistentAmazonS3.getClient(), _bucket, blockKey);
      }
      LOGGER.info("keys {}", keys);
      if (keys.isEmpty()) {
        return Block.MISSING_BLOCK_GENERATION;
      }
      List<Long> generations = getGenerations(keys);
      LOGGER.info("generations {}", generations);
      return Collections.max(generations);
    };
    _cache = Caffeine.newBuilder()
                     .executor(ExecutorUtil.getCallerRunExecutor())
                     .build(loader);
  }

  @Override
  public void preloadGenerationInfo(long volumeId, long numberOfBlocks) throws IOException {
    String volumeKey = S3Utils.getVolumeKeyPrefix(_objectPrefix, volumeId);
    try (Scope scope = TracerUtil.trace(S3GenerationBlockStore.class, "s3 list objects")) {
      S3Utils.listObjects(_consistentAmazonS3.getClient(), _bucket, volumeKey, new ListResultProcessor() {
        @Override
        public void addResult(S3ObjectSummary summary) {
          String key = summary.getKey();
          if (key.contains("metadata")) {
            return;
          }
          List<String> list = KEY_SPLITTER.splitToList(key);
          long blockId = getBlockId(list);
          if (blockId < 0) {
            return;
          }
          long currentGeneration = getGeneration(list);
          BlockKey blockKey = BlockKey.builder()
                                      .blockId(blockId)
                                      .volumeId(volumeId)
                                      .build();
          Long existingGeneration = _cache.getIfPresent(blockKey);
          if (existingGeneration == null || currentGeneration > existingGeneration) {
            LOGGER.info("preload {} with generation {}", blockKey, currentGeneration);
            _cache.put(blockKey, currentGeneration);
          }
        }

        private long getGeneration(List<String> list) {
          return Long.parseLong(list.get(list.size() - 1));
        }

        private long getBlockId(List<String> list) {
          try {
            return Long.parseLong(list.get(list.size() - 2));
          } catch (NumberFormatException e) {
            return -1L;
          }
        }
      });
    }

    for (long blockId = 0; blockId < numberOfBlocks; blockId++) {
      BlockKey blockKey = BlockKey.builder()
                                  .blockId(blockId)
                                  .volumeId(volumeId)
                                  .build();
      Long generation = _cache.getIfPresent(blockKey);
      if (generation == null) {
        _cache.put(blockKey, Block.MISSING_BLOCK_GENERATION);
      }
    }
  }

  @Override
  public long getLastStoreGeneration(long volumeId, long blockId) throws IOException {
    return _cache.get(BlockKey.builder()
                              .volumeId(volumeId)
                              .blockId(blockId)
                              .build());
  }

  @Override
  public void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException {
    LOGGER.info("storing generation volumeId {} blockId {} lastStoredGeneration {}", volumeId, blockId,
        lastStoredGeneration);
    _cache.put(BlockKey.builder()
                       .volumeId(volumeId)
                       .blockId(blockId)
                       .build(),
        lastStoredGeneration);
  }

  private List<Long> getGenerations(List<String> keys) {
    List<Long> generations = new ArrayList<>();
    for (String key : keys) {
      List<String> list = KEY_SPLITTER.splitToList(key);
      String generationStr = list.get(list.size() - 1);
      generations.add(Long.parseLong(generationStr));
    }
    return generations;
  }
}
