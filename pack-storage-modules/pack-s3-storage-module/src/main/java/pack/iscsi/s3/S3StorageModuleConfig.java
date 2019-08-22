package pack.iscsi.s3;

import com.github.benmanes.caffeine.cache.LoadingCache;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class S3StorageModuleConfig {
  String name;
  long lengthInBytes;
  int blockSize;
  LoadingCache<S3CacheKey, S3Block> cache;
  String s3Bucket;
  String s3ObjectPrefix;
}
