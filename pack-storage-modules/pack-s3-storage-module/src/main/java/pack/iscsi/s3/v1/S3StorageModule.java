package pack.iscsi.s3.v1;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.s3.v1.S3Block.S3BlockIOExecutor;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;

public class S3StorageModule implements StorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageModule.class);

  public static class S3StorageModuleFactory implements StorageModuleFactory {

    private final LoadingCache<S3CacheKey, S3Block> _cache;
    private final ConsistentAmazonS3 _consistentAmazonS3;
    private final File _volumesDir;
    private final String _s3Bucket;
    private final String _s3ObjectPrefix;

    public S3StorageModuleFactory(S3StorageModuleFactoryConfig config) {
      _s3Bucket = config.getS3Bucket();
      _s3ObjectPrefix = config.getS3ObjectPrefix();
      _consistentAmazonS3 = config.getConsistentAmazonS3();
      _volumesDir = config.getVolumesDir();
      _volumesDir.mkdirs();

      CacheLoader<S3CacheKey, S3Block> loader = getCacheLoader();
      RemovalListener<S3CacheKey, S3Block> removalListener = getRemovalListener();
      Weigher<S3CacheKey, S3Block> weigher = getWeigher();
      _cache = Caffeine.newBuilder()
                       .removalListener(removalListener)
                       .weigher(weigher)
                       .maximumWeight(config.getCacheSizeInBytes())
                       .build(loader);

    }

    private Weigher<S3CacheKey, S3Block> getWeigher() {
      return (key, value) -> value.getBlockSize();
    }

    private RemovalListener<S3CacheKey, S3Block> getRemovalListener() {
      return (key, value, cause) -> {
        value.exec(new S3UploadBlockIO(_consistentAmazonS3, key, value, true));
      };
    }

    private CacheLoader<S3CacheKey, S3Block> getCacheLoader() {
      return key -> {
        File blockFile = getBlockFileLocation(key);
        GetObjectRequest getRequest = new GetObjectRequest(key.getS3Bucket(), key.getS3Key());
        while (true) {
          try {
            _consistentAmazonS3.getObject(getRequest, blockFile);
            return S3Block.createExisting(blockFile, key.getBlockSize());
          } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404) {
              return S3Block.createEmptyBlock(blockFile, key.getBlockSize());
            }
            LOGGER.error("Unknown error, trying", e);
            Thread.sleep(TimeUnit.SECONDS.toMillis(3));
          }
        }
      };
    }

    @Override
    public List<String> getStorageModuleNames() {
      return Arrays.asList("test");
    }

    @Override
    public S3StorageModule getStorageModule(String name) throws IOException {
      S3StorageModuleConfig config = S3StorageModuleConfig.builder()
                                                          .name(name)
                                                          .cache(_cache)
                                                          .blockSize(1_000_000)
                                                          .lengthInBytes(100_000_000_000L)
                                                          .s3Bucket(_s3Bucket)
                                                          .s3ObjectPrefix(_s3ObjectPrefix)
                                                          .build();
      return new S3StorageModule(config);
    }

    private File getBlockFileLocation(S3CacheKey key) {
      File volumeDir = new File(_volumesDir, key.getName());
      volumeDir.mkdirs();
      return new File(volumeDir, Long.toString(key.getBlockId()));
    }

    public Map<S3CacheKey, S3Block> getCacheAsMap() {
      return _cache.asMap();
    }

  }

  public static S3StorageModuleFactory createFactory(S3StorageModuleFactoryConfig config) {
    return new S3StorageModuleFactory(config);
  }

  private final int _blockSize;
  private final LoadingCache<S3CacheKey, S3Block> _cache;
  private final String _name;
  private final long _lengthInBytes;
  private String _s3Bucket;
  private String _s3ObjectPrefix;

  public S3StorageModule(S3StorageModuleConfig config) throws IOException {
    _s3Bucket = config.getS3Bucket();
    _s3ObjectPrefix = config.getS3ObjectPrefix();
    _name = config.getName();
    _cache = config.getCache();
    _blockSize = config.getBlockSize();
    _lengthInBytes = config.getLengthInBytes();
  }

  @Override
  public void read(byte[] bytes, long position) throws IOException {
    int length = bytes.length;
    int offset = 0;
    while (length > 0) {
      long blockId = getBlockId(position);
      int blockOffset = getBlockOffset(position);
      int remaining = _blockSize - blockOffset;
      S3CacheKey key = S3CacheKey.builder()
                                 .s3Bucket(_s3Bucket)
                                 .s3ObjectPrefix(_s3ObjectPrefix)
                                 .name(_name)
                                 .blockId(blockId)
                                 .blockSize(_blockSize)
                                 .build();
      S3Block s3CacheValue = _cache.get(key);
      int len = Math.min(remaining, length);
      s3CacheValue.readFully(blockOffset, bytes, offset, len);
      length -= len;
      position += len;
      offset += len;
    }
  }

  @Override
  public void write(byte[] bytes, long position) throws IOException {
    int length = bytes.length;
    int offset = 0;
    while (length > 0) {
      long blockId = getBlockId(position);
      int blockOffset = getBlockOffset(position);
      int remaining = _blockSize - blockOffset;
      S3CacheKey key = S3CacheKey.builder()
                                 .s3Bucket(_s3Bucket)
                                 .s3ObjectPrefix(_s3ObjectPrefix)
                                 .name(_name)
                                 .blockId(blockId)
                                 .blockSize(_blockSize)
                                 .build();
      S3Block s3CacheValue = _cache.get(key);
      int len = Math.min(remaining, length);
      s3CacheValue.writeFully(blockOffset, bytes, offset, len);
      length -= len;
      position += len;
      offset += len;
    }
  }

  private int getBlockOffset(long position) {
    return (int) (position % _blockSize);
  }

  private long getBlockId(long position) {
    return position / _blockSize;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public final int checkBounds(final long logicalBlockAddress, final int transferLengthInBlocks) {
    if (logicalBlockAddress < 0 || logicalBlockAddress > getBlockCount()) {
      return 1;
    } else if (transferLengthInBlocks < 0 || logicalBlockAddress + transferLengthInBlocks > getBlockCount()) {
      return 2;
    } else {
      return 0;
    }
  }

  @Override
  public long getSizeInBlocks() {
    return getBlockCount() - 1;
  }

  private long getBlockCount() {
    return _lengthInBytes / VIRTUAL_BLOCK_SIZE;
  }

}
