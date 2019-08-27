package pack.iscsi.external.s3;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.partitioned.block.BlockIOExecutor;
import pack.iscsi.partitioned.block.BlockIORequest;
import pack.iscsi.partitioned.block.BlockIOResponse;
import pack.iscsi.partitioned.block.BlockState;

public class S3BlockWriter implements BlockIOExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BlockWriter.class);

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;

  public S3BlockWriter(ConsistentAmazonS3 consistentAmazonS3, String bucket, String objectPrefix) {
    _consistentAmazonS3 = consistentAmazonS3;
    _bucket = bucket;
    _objectPrefix = objectPrefix;
  }

  @Override
  public BlockIOResponse exec(BlockIORequest request) throws IOException {
    File file = request.getFileForReadingOnly();
    long onDiskGeneration = request.getOnDiskGeneration();
    BlockState onDiskState = request.getOnDiskState();
    long lastStoredGeneration = request.getLastStoredGeneration();
    try {
      String key = S3Utils.getKey(_objectPrefix, request);
      _consistentAmazonS3.putObject(_bucket, key, file);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return BlockIOResponse.newBlockIOResult(onDiskGeneration, onDiskState, lastStoredGeneration);
    }
    return BlockIOResponse.newBlockIOResult(onDiskGeneration, BlockState.CLEAN, onDiskGeneration);
  }

}
