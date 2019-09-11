package pack.iscsi.s3.block;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class S3ExternalBlockStoreFactoryConfig {
  ConsistentAmazonS3 consistentAmazonS3;
  String bucket;
  String objectPrefix;
}
