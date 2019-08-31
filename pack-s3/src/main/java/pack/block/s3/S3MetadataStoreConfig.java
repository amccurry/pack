package pack.block.s3;

import com.amazonaws.services.s3.AmazonS3;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class S3MetadataStoreConfig {

  AmazonS3 client;
  ConsistentAmazonS3 consistentS3Client;
  String bucketName;
  String prefix;

}
