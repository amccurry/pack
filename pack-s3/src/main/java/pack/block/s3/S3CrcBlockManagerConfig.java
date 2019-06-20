package pack.block.s3;

import com.amazonaws.services.s3.AmazonS3;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class S3CrcBlockManagerConfig {

  AmazonS3 client;
  String bucketName;
  String prefix;
  
}
