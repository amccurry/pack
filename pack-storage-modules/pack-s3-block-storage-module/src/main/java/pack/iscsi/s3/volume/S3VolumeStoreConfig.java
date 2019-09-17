package pack.iscsi.s3.volume;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class S3VolumeStoreConfig {
  ConsistentAmazonS3 consistentAmazonS3;
  String bucket;
  String objectPrefix;
}
