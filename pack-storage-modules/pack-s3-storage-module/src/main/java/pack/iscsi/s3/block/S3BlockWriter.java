package pack.iscsi.s3.block;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectMetadata;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;
import pack.iscsi.s3.util.S3Utils;
import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.spi.block.BlockIORequest;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;

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
    long onDiskGeneration = request.getOnDiskGeneration();
    BlockState onDiskState = request.getOnDiskState();
    long lastStoredGeneration = request.getLastStoredGeneration();
    if (onDiskState == BlockState.CLEAN) {
      return BlockIOResponse.newBlockIOResult(onDiskGeneration, onDiskState, lastStoredGeneration);
    }
    try {
      String key = S3Utils.getBlockGenerationKey(_objectPrefix, request.getVolumeId(), request.getBlockId(),
          onDiskGeneration);
      LOGGER.info("writing bucket {} key {}", _bucket, key);
      InputStream input = getInputStream(request.getChannel(), request.getBlockSize());
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(request.getBlockSize());
      _consistentAmazonS3.putObject(_bucket, key, input, metadata);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      return BlockIOResponse.newBlockIOResult(onDiskGeneration, onDiskState, lastStoredGeneration);
    }
    return BlockIOResponse.newBlockIOResult(onDiskGeneration, BlockState.CLEAN, onDiskGeneration);
  }

  private InputStream getInputStream(FileChannel channel, int blockSize) {
    InputStream input = new InputStream() {

      private long _position = 0;

      @Override
      public int read() throws IOException {
        if (_position >= blockSize) {
          return -1;
        }
        ByteBuffer dst = ByteBuffer.allocate(1);
        while (dst.remaining() > 0) {
          _position += channel.read(dst, _position);
        }
        return dst.get();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (_position >= blockSize) {
          return -1;
        }
        int length = (int) Math.min(len, blockSize - _position);
        ByteBuffer buffer = ByteBuffer.wrap(b, off, length);
        int read = channel.read(buffer, _position);
        _position += read;
        return read;
      }
    };
    return new BufferedInputStream(input, com.amazonaws.RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE);
  }

}
