package pack.iscsi.external.s3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Splitter;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.storagemanager.BlockKey;
import pack.iscsi.partitioned.storagemanager.BlockGenerationStore;

public class S3BlockStore implements BlockGenerationStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BlockStore.class);

  private static final Splitter KEY_SPLITTER = Splitter.on('/');
  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;
  private final LoadingCache<BlockKey, Long> _cache;

  public S3BlockStore(S3BlockStoreConfig config) {
    _consistentAmazonS3 = config.getConsistentAmazonS3();
    _bucket = config.getBucket();
    _objectPrefix = config.getObjectPrefix();

    CacheLoader<BlockKey, Long> loader = key -> {
      String blockKey = S3Utils.getBlockKeyPrefix(_objectPrefix, key.getVolumeId(), key.getBlockId());
      LOGGER.info("blockkey {} scan key from {}", key, blockKey);
      List<String> keys = S3Utils.listObjects(_consistentAmazonS3.getClient(), _bucket, blockKey);
      LOGGER.info("keys {}", keys);
      if (keys.isEmpty()) {
        return Block.MISSING_BLOCK_GENERATION;
      }
      List<Long> generations = getGenerations(keys);
      LOGGER.info("generations {}", generations);
      return Collections.max(generations);
    };
    _cache = Caffeine.newBuilder()
                     .expireAfterWrite(config.getExpireTimeAfterWrite(), config.getExpireTimeAfterWriteTimeUnit())
                     .build(loader);
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
