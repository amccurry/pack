package pack.block.s3;

import java.io.File;

import com.amazonaws.services.s3.AmazonS3;

import lombok.Builder;
import lombok.Value;
import pack.block.BlockConfig;

@Value
@Builder(toBuilder = true)
public class S3BlockConfig {

  BlockConfig blockConfig;

  AmazonS3 client;
  String bucketName;
  String prefix;
  long consistencyWaitTime;
  File localCacheFile;
  File uploadDir;

}
