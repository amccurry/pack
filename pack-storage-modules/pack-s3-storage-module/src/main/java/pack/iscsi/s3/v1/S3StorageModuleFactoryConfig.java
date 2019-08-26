package pack.iscsi.s3.v1;

import java.io.File;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class S3StorageModuleFactoryConfig {
  File volumesDir;
  ConsistentAmazonS3 consistentAmazonS3;
  long cacheSizeInBytes;
  String s3Bucket;
  String s3ObjectPrefix;
}
