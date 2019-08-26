package pack.iscsi.s3;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.partitioned.block.BlockIOExecutor;
import pack.iscsi.partitioned.block.BlockIOResult;
import pack.iscsi.partitioned.block.BlockState;

public class S3BlockReader implements BlockIOExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BlockReader.class);

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _key;

  public S3BlockReader(ConsistentAmazonS3 consistentAmazonS3, String bucket, String key) {
    _consistentAmazonS3 = consistentAmazonS3;
    _bucket = bucket;
    _key = key;
  }

  @Override
  public BlockIOResult exec(FileChannel channel, File file, int blockSize, long onDiskGeneration,
      BlockState onDiskState, long lastStoredGeneration) throws IOException {
    try {
      S3Object s3Object = _consistentAmazonS3.getObject(_bucket, getKey(lastStoredGeneration));
      try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
        byte[] buffer = new byte[4096];
        int read;
        long pos = 0;
        while ((read = inputStream.read(buffer)) != -1) {
          pos += channel.write(ByteBuffer.wrap(buffer, 0, read), pos);
        }
      }
      return BlockIOResult.newBlockIOResult(lastStoredGeneration, BlockState.CLEAN, lastStoredGeneration);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return BlockIOResult.newBlockIOResult(onDiskGeneration, onDiskState, lastStoredGeneration);
    }
  }

  private String getKey(long generation) {
    return _key + "/" + generation;
  }

}
