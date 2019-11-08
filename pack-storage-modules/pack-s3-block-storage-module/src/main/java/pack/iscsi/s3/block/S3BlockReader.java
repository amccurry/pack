package pack.iscsi.s3.block;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import consistent.s3.ConsistentAmazonS3;
import io.opentracing.Scope;
import lombok.Builder;
import lombok.Value;
import pack.iscsi.s3.util.S3Utils;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.spi.block.BlockIORequest;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.metric.Meter;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.util.tracer.TracerUtil;

public class S3BlockReader implements BlockIOExecutor {

  @Value
  @Builder(toBuilder = true)
  public static class S3BlockReaderConfig {
    ConsistentAmazonS3 consistentAmazonS3;
    String bucket;
    String objectPrefix;
    @Builder.Default
    MetricsFactory metricsFactory = MetricsFactory.NO_OP;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BlockReader.class);

  private static final String READ = "s3-bytes|read";

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;
  private final MetricsFactory _metricsFactory;

  public S3BlockReader(S3BlockReaderConfig config) {
    _consistentAmazonS3 = config.getConsistentAmazonS3();
    _bucket = config.getBucket();
    _objectPrefix = config.getObjectPrefix();
    _metricsFactory = config.getMetricsFactory();
  }

  @Override
  public BlockIOResponse exec(BlockIORequest request) throws IOException {
    try (Scope s0 = TracerUtil.trace(getClass(), "s3 read")) {
      // @TODO partial reads may cause corruption, needs work
      long lastStoredGeneration = request.getLastStoredGeneration();
      if (lastStoredGeneration == Block.MISSING_BLOCK_GENERATION
          || lastStoredGeneration == request.getOnDiskGeneration()) {
        return BlockIOResponse.newBlockIOResult(lastStoredGeneration, BlockState.CLEAN, lastStoredGeneration);
      }
      RandomAccessIO randomAccessIO = request.getRandomAccessIO();
      String key = S3Utils.getBlockGenerationKey(_objectPrefix, request.getVolumeId(), request.getBlockId(),
          lastStoredGeneration);
      LOGGER.debug("reading bucket {} key {}", _bucket, key);
      S3Object s3Object;
      try (Scope s1 = TracerUtil.trace(getClass(), "s3 read object")) {
        s3Object = _consistentAmazonS3.getObject(_bucket, key);
      }
      long contentLength = s3Object.getObjectMetadata()
                                   .getContentLength();
      int blockSize = request.getBlockSize();
      if (contentLength != blockSize) {
        LOGGER.error("object size wrong bucket {} key {} content length {} blocksize {}", _bucket, key, contentLength,
            blockSize);
        throw new IOException("object size wrong");
      }
      try (Scope s1 = TracerUtil.trace(getClass(), "s3 read content")) {
        Meter readMeter = _metricsFactory.meter(S3BlockReader.class, Long.toString(request.getVolumeId()), READ);
        try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
          byte[] buffer = new byte[128 * 1024];
          long pos = request.getStartingPositionOfBlock();
          int length = blockSize;
          while (length > 0) {
            int len = Math.min(length, buffer.length);
            int read;
            try (Scope s2 = TracerUtil.trace(getClass(), "s3 read content inputstream")) {
              read = inputStream.read(buffer, 0, len);
            }
            try (Scope s2 = TracerUtil.trace(getClass(), "write content")) {
              randomAccessIO.write(pos, buffer, 0, read);
            }
            readMeter.mark(read);
            pos += read;
            length -= read;
          }
        }
      }
      return BlockIOResponse.newBlockIOResult(lastStoredGeneration, BlockState.CLEAN, lastStoredGeneration);
    }
  }

}
