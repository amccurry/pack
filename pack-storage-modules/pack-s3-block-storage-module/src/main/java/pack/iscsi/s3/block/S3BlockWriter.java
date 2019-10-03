package pack.iscsi.s3.block;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectMetadata;

import consistent.s3.ConsistentAmazonS3;
import io.opencensus.common.Scope;
import lombok.Builder;
import lombok.Value;
import pack.iscsi.s3.util.S3Utils;
import pack.iscsi.spi.RandomAccessIOReader;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.spi.block.BlockIORequest;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;
import pack.util.TracerUtil;

public class S3BlockWriter implements BlockIOExecutor {

  @Value
  @Builder(toBuilder = true)
  public static class S3BlockWriterConfig {
    ConsistentAmazonS3 consistentAmazonS3;
    String bucket;
    String objectPrefix;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BlockWriter.class);

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;

  public S3BlockWriter(S3BlockWriterConfig config) {
    _consistentAmazonS3 = config.getConsistentAmazonS3();
    _bucket = config.getBucket();
    _objectPrefix = config.getObjectPrefix();
  }

  @Override
  public BlockIOResponse exec(BlockIORequest request) throws IOException {
    try (Scope scope = TracerUtil.trace(getClass(), "s3 write")) {
      long onDiskGeneration = request.getOnDiskGeneration();
      BlockState onDiskState = request.getOnDiskState();
      long lastStoredGeneration = request.getLastStoredGeneration();
      if (onDiskState == BlockState.CLEAN || onDiskGeneration == Block.MISSING_BLOCK_GENERATION
          || lastStoredGeneration == onDiskGeneration) {
        return BlockIOResponse.newBlockIOResult(onDiskGeneration, BlockState.CLEAN, lastStoredGeneration);
      }
      String key = S3Utils.getBlockGenerationKey(_objectPrefix, request.getVolumeId(), request.getBlockId(),
          onDiskGeneration);
      LOGGER.info("starting write bucket {} key {}", _bucket, key);
      try (RandomAccessIOReader reader = request.getRandomAccessIO()
                                                .cloneReadOnly()) {
        InputStream input = getInputStream(reader, request.getBlockSize(), request.getStartingPositionOfBlock());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(request.getBlockSize());
        _consistentAmazonS3.putObject(_bucket, key, input, metadata);
      }
      LOGGER.info("finished write bucket {} key {}", _bucket, key);
      return BlockIOResponse.newBlockIOResult(onDiskGeneration, BlockState.CLEAN, onDiskGeneration);
    }
  }

  private InputStream getInputStream(RandomAccessIOReader reader, int blockSize, long startingPositionOfBlock) {
    InputStream input = new InputStream() {

      private long _position = 0;

      @Override
      public int read() throws IOException {
        if (_position >= blockSize) {
          return -1;
        }
        byte[] buffer = new byte[1];
        reader.readFully(_position + startingPositionOfBlock, buffer);
        _position++;
        return buffer[0];
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (_position >= blockSize) {
          return -1;
        }
        int length = (int) Math.min(len, blockSize - _position);
        reader.readFully(_position + startingPositionOfBlock, b, off, length);
        _position += length;
        return length;
      }
    };
    return new BufferedInputStream(input, com.amazonaws.RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE);
  }

}
