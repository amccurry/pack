package pack.block.s3;

import java.io.File;

import com.amazonaws.services.s3.AmazonS3;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class S3BlockFactoryConfig {

  String bucketName;
  AmazonS3 client;
  File cacheDir;
  String prefix;
  File uploadDir;

}
