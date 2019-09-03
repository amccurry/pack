package pack.iscsi.external.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.block.BlockIOExecutor;
import pack.iscsi.partitioned.block.BlockIORequest;
import pack.iscsi.partitioned.block.BlockIOResponse;
import pack.iscsi.partitioned.block.BlockState;

public class S3BlockReader implements BlockIOExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BlockReader.class);

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;

  public S3BlockReader(ConsistentAmazonS3 consistentAmazonS3, String bucket, String objectPrefix) {
    _consistentAmazonS3 = consistentAmazonS3;
    _bucket = bucket;
    _objectPrefix = objectPrefix;
  }

  @Override
  public BlockIOResponse exec(BlockIORequest request) throws IOException {

    // @TODO partial reads may cause corruption, needs work
    long lastStoredGeneration = request.getLastStoredGeneration();
    if (lastStoredGeneration == Block.MISSING_BLOCK_GENERATION) {
      return BlockIOResponse.newBlockIOResult(lastStoredGeneration, BlockState.CLEAN, lastStoredGeneration);
    }

    FileChannel channel = request.getChannel();
    long onDiskGeneration = request.getOnDiskGeneration();
    BlockState onDiskState = request.getOnDiskState();
    try {
      String key = S3Utils.getBlockGenerationKey(_objectPrefix, request.getVolumeId(), request.getBlockId(), lastStoredGeneration);
      S3Object s3Object = _consistentAmazonS3.getObject(_bucket, key);
      try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
        byte[] buffer = new byte[4096];
        int read;
        long pos = 0;
        while ((read = inputStream.read(buffer)) != -1) {
          pos += channel.write(ByteBuffer.wrap(buffer, 0, read), pos);
        }
      }
      return BlockIOResponse.newBlockIOResult(lastStoredGeneration, BlockState.CLEAN, lastStoredGeneration);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return BlockIOResponse.newBlockIOResult(onDiskGeneration, onDiskState, lastStoredGeneration);
    }
  }

}
