package pack.s3;

import java.io.File;
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.s3.AmazonS3;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class S3BlockConfig {

  AmazonS3 client;
  String bucketName;
  String prefix;
  long blockSize;
  long blockId;
  long currentCrc;
  long idleWriteTime;
  File localCacheFile;
  ExecutorService service;

}
