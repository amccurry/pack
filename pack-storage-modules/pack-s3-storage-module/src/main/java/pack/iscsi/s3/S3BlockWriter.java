package pack.iscsi.s3;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.partitioned.block.BlockIOExecutor;
import pack.iscsi.partitioned.block.BlockIOResult;
import pack.iscsi.partitioned.block.BlockState;

public class S3BlockWriter implements BlockIOExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BlockWriter.class);

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _key;

  public S3BlockWriter(ConsistentAmazonS3 consistentAmazonS3, String bucket, String key) {
    _consistentAmazonS3 = consistentAmazonS3;
    _bucket = bucket;
    _key = key;
  }

  @Override
  public BlockIOResult exec(FileChannel channel, File file, int blockSize, long onDiskGeneration,
      BlockState onDiskState, long lastStoredGeneration) throws IOException {
    try {
      _consistentAmazonS3.putObject(_bucket, getKey(onDiskGeneration), file);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return BlockIOResult.newBlockIOResult(onDiskGeneration, onDiskState, lastStoredGeneration);
    }
    return BlockIOResult.newBlockIOResult(onDiskGeneration, BlockState.CLEAN, onDiskGeneration);
  }

  private String getKey(long generation) {
    return _key + "/" + generation;
  }

}
