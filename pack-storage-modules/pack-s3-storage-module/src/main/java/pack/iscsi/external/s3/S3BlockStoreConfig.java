package pack.iscsi.external.s3;

import java.util.concurrent.TimeUnit;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class S3BlockStoreConfig {
  ConsistentAmazonS3 consistentAmazonS3;
  String bucket;
  String objectPrefix;

  @Builder.Default
  long expireTimeAfterWrite = 10;

  @Builder.Default
  TimeUnit expireTimeAfterWriteTimeUnit = TimeUnit.MINUTES;

}
