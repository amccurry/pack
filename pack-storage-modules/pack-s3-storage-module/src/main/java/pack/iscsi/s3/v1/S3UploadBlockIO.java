package pack.iscsi.s3.v1;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectMetadata;

import consistent.s3.ConsistentAmazonS3;

public class S3UploadBlockIO implements S3BlockIOExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3UploadBlockIO.class);

  private final S3Block _blockCache;
  private final S3CacheKey _key;
  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final boolean _clearlocalData;

  public S3UploadBlockIO(ConsistentAmazonS3 consistentAmazonS3, S3CacheKey key, S3Block blockCache,
      boolean clearlocalData) {
    _consistentAmazonS3 = consistentAmazonS3;
    _key = key;
    _blockCache = blockCache;
    _clearlocalData = clearlocalData;
  }

  @Override
  public S3BlockState exec(FileChannel channel, long positionOfStartOfBlock, int blockSize) throws IOException {
    if (_blockCache.isDirty()) {
      LOGGER.info("Dirty block removed, uploading {}", _key);
      uploadForEverUntilSuccess(_key.getS3Bucket(), _key.getS3Key(), channel, positionOfStartOfBlock, blockSize);
    }
    if (_clearlocalData) {
      _blockCache.clearLocalData();
      return S3BlockState.MISSING;
    } else {
      return S3BlockState.CLEAN;
    }
  }

  private void uploadForEverUntilSuccess(String s3Bucket, String s3Key, FileChannel channel,
      long positionOfStartOfBlock, int blockSize) {
    while (true) {
      try {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(blockSize);
        try (InputStream input = getFileChannelInputStream(channel, positionOfStartOfBlock, blockSize)) {
          _consistentAmazonS3.putObject(s3Bucket, s3Key, input, metadata);
        }
      } catch (Exception e) {
        LOGGER.error("Unknown error", e);
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        } catch (InterruptedException ex) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private InputStream getFileChannelInputStream(FileChannel channel, long positionOfStartOfBlock, int blockSize) {
    // inputstream needs to work with client

    return null;
  }

}
