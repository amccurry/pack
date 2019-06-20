package pack.block;

import com.amazonaws.services.s3.AmazonS3;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class CrcBlockManagerConfig {
  
  AmazonS3 client;
  String bucketName;
  String volumeName;
  long volumeSize;
  long blockSize;
  
}
