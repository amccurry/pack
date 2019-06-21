package pack.block.s3;

import com.amazonaws.services.s3.AmazonS3;

import lombok.Builder;
import lombok.Value;
import pack.block.CrcBlockManager;

@Value
@Builder(toBuilder = true)
public class S3CrcBlockManagerConfig {

  CrcBlockManager crcBlockManager;

  AmazonS3 client;
  String bucketName;
  String prefix;
  String volume;

}
