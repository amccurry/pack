package pack.iscsi.s3;

import java.nio.channels.FileChannel;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class S3BlockCacheManager {

  private final LoadingCache<S3CacheKey, S3Block> _cache;

  public S3BlockCacheManager() {
    CacheLoader<S3CacheKey, S3Block> loader = new CacheLoader<S3CacheKey, S3Block>() {
      @Override
      public S3Block load(S3CacheKey key) throws Exception {
        return new S3Block(getChannel(key.getName()), key.getBlockId(), key.getBlockSize());
      }
    };
    _cache = Caffeine.newBuilder()
                     .weakValues()
                     .build(loader);
  }

  public S3Block getS3BlockCache(S3CacheKey key) {
    return _cache.get(key);
  }

  private FileChannel getChannel(String name) {
    // TODO Auto-generated method stub
    return null;
  }

}
