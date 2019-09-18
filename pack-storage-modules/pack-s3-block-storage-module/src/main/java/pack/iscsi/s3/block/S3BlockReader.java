package pack.iscsi.s3.block;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;
import pack.iscsi.s3.util.S3Utils;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.spi.block.BlockIORequest;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;

public class S3BlockReader implements BlockIOExecutor {

  @Value
  @Builder(toBuilder = true)
  public static class S3BlockReaderConfig {
    ConsistentAmazonS3 consistentAmazonS3;
    String bucket;
    String objectPrefix;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BlockReader.class);

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;

  public S3BlockReader(S3BlockReaderConfig config) {
    _consistentAmazonS3 = config.getConsistentAmazonS3();
    _bucket = config.getBucket();
    _objectPrefix = config.getObjectPrefix();
  }

  @Override
  public BlockIOResponse exec(BlockIORequest request) throws IOException {

    // @TODO partial reads may cause corruption, needs work
    long lastStoredGeneration = request.getLastStoredGeneration();
    if (lastStoredGeneration == Block.MISSING_BLOCK_GENERATION) {
      return BlockIOResponse.newBlockIOResult(lastStoredGeneration, BlockState.CLEAN, lastStoredGeneration);
    }

    RandomAccessIO randomAccessIO = request.getRandomAccessIO();
    long onDiskGeneration = request.getOnDiskGeneration();
    BlockState onDiskState = request.getOnDiskState();
    try {
      String key = S3Utils.getBlockGenerationKey(_objectPrefix, request.getVolumeId(), request.getBlockId(),
          lastStoredGeneration);
      LOGGER.info("reading bucket {} key {}", _bucket, key);
      S3Object s3Object = _consistentAmazonS3.getObject(_bucket, key);
      try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
        byte[] buffer = new byte[4096];
        int read;
        long pos = 0;
        while ((read = inputStream.read(buffer)) != -1) {
          randomAccessIO.writeFully(pos, buffer, 0, read);
          pos += read;
        }
      }
      return BlockIOResponse.newBlockIOResult(lastStoredGeneration, BlockState.CLEAN, lastStoredGeneration);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return BlockIOResponse.newBlockIOResult(onDiskGeneration, onDiskState, lastStoredGeneration);
    }
  }

}