package pack.iscsi.external.s3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectMetadata;

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
    long onDiskGeneration = request.getOnDiskGeneration();
    BlockState onDiskState = request.getOnDiskState();
    long lastStoredGeneration = request.getLastStoredGeneration();
    if (onDiskState == BlockState.CLEAN) {
      return BlockIOResponse.newBlockIOResult(onDiskGeneration, onDiskState, lastStoredGeneration);
    }
    try {
      String key = S3Utils.getKey(_objectPrefix, request);
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

      private long position = 0;

      @Override
      public int read() throws IOException {
        if (position >= blockSize) {
          return -1;
        }
        ByteBuffer dst = ByteBuffer.allocate(1);
        while (dst.remaining() > 0) {
          position += channel.read(dst, position);
        }
        return dst.get();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (position >= blockSize) {
          return -1;
        }
        int length = (int) Math.min(len, blockSize - position);
        ByteBuffer buffer = ByteBuffer.wrap(b, off, length);
        int read = channel.read(buffer, position);
        read += position;
        return read;
      }

    };
    return new BufferedInputStream(input, com.amazonaws.RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE);
  }

}
