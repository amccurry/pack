package pack.s3;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import jnr.ffi.Pointer;

public class S3FileHandleV2 implements FileHandle {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3FileHandleV2.class);

  private final AmazonS3 _client;
  private final long _blockSize;
  private final String _bucket;
  private final File _cacheDir;
  private final String _volumeName;
  private final LoadingCache<Long, S3Block> _cache;
  private final S3VolumeMetaDataManager _metaDataManager;
  private final S3VolumeMetaData _metaData;
  private final ExecutorService _service;

  public S3FileHandleV2(String bucket, String volumeName, long blockSize, String cache,
      S3VolumeMetaDataManager metaDataManager) throws IOException {
    _metaDataManager = metaDataManager;
    _metaData = _metaDataManager.getMetaData(volumeName);
    _cacheDir = new File(cache);
    _bucket = bucket;
    _volumeName = volumeName;
    _blockSize = blockSize;
    _client = AmazonS3ClientBuilder.defaultClient();
    _service = Executors.newCachedThreadPool();

    S3BlockConfig baseConfig = S3BlockConfig.builder()
                                            .blockSize(_blockSize)
                                            .bucketName(_bucket)
                                            .client(_client)
                                            .prefix(_volumeName)
                                            .service(_service)
                                            .idleWriteTime(TimeUnit.SECONDS.toNanos(10))
                                            .build();

    RemovalListener<Long, S3Block> listener = new RemovalListener<Long, S3Block>() {
      @Override
      public void onRemoval(RemovalNotification<Long, S3Block> notification) {
        S3Block block = notification.getValue();
        Long blockId = notification.getKey();
        LOGGER.info("Remove block {} from cache", blockId);
        long sync = block.sync();
        _metaData.setCrc(blockId, sync);
        block.close();
        S3BlockConfig config = block.getConfig();
        config.getLocalCacheFile()
              .delete();
      }
    };
    CacheLoader<Long, S3Block> loader = new CacheLoader<Long, S3Block>() {
      @Override
      public S3Block load(Long blockId) throws Exception {
        long currentCrc = _metaData.getCrc(blockId);
        File localCacheFile = new File(_cacheDir, UUID.randomUUID()
                                                      .toString());
        S3BlockConfig config = baseConfig.toBuilder()
                                         .blockId(blockId)
                                         .localCacheFile(localCacheFile)
                                         .currentCrc(currentCrc)
                                         .build();

        LOGGER.info("Loading block {} into cache", blockId);

        return new S3Block(config);
      }
    };
    _cache = CacheBuilder.newBuilder()
                         .removalListener(listener)
                         .maximumSize(100)
                         .build(loader);
    LOGGER.info("Volume {} opened", _volumeName);
  }

  public void sync() throws IOException {
    _metaDataManager.setMetaData(_volumeName, _metaData);
  }

  @Override
  public int read(Pointer buf, int size, long offset) throws Exception {
    long blockId = getBlockId(offset);
    long relativeOffset = getRelativeOffset(offset);
    S3Block block = _cache.get(blockId);
    return block.read(buf, size, relativeOffset);
  }

  private long getRelativeOffset(long offset) {
    return offset % _blockSize;
  }

  @Override
  public int write(Pointer buf, int size, long offset) throws Exception {
    long blockId = getBlockId(offset);
    long relativeOffset = getRelativeOffset(offset);
    S3Block block = _cache.get(blockId);
    return block.write(buf, size, relativeOffset);
  }

  private long getBlockId(long offset) {
    return offset / _blockSize;
  }

  @Override
  public void close() throws IOException {
    _cache.invalidateAll();
    sync();
    LOGGER.info("Volume {} closed", _volumeName);
    _service.shutdownNow();
  }

}
