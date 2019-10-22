package pack.iscsi.s3.block;

import java.io.IOException;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;
import pack.iscsi.s3.block.S3BlockReader.S3BlockReaderConfig;
import pack.iscsi.s3.block.S3BlockWriter.S3BlockWriterConfig;
import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.metric.MetricsFactory;

public class S3ExternalBlockStoreFactory implements BlockIOFactory {

  @Value
  @Builder(toBuilder = true)
  public static class S3ExternalBlockStoreFactoryConfig {
    ConsistentAmazonS3 consistentAmazonS3;
    String bucket;
    String objectPrefix;
    @Builder.Default
    MetricsFactory metricsFactory = MetricsFactory.NO_OP;
  }

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;
  private final MetricsFactory _metricsFactory;

  public S3ExternalBlockStoreFactory(S3ExternalBlockStoreFactoryConfig config) {
    _consistentAmazonS3 = config.getConsistentAmazonS3();
    _bucket = config.getBucket();
    _objectPrefix = config.getObjectPrefix();
    _metricsFactory = config.getMetricsFactory();
  }

  @Override
  public BlockIOExecutor getBlockWriter() throws IOException {
    S3BlockWriterConfig config = S3BlockWriterConfig.builder()
                                                    .bucket(_bucket)
                                                    .consistentAmazonS3(_consistentAmazonS3)
                                                    .objectPrefix(_objectPrefix)
                                                    .metricsFactory(_metricsFactory)
                                                    .build();
    return new S3BlockWriter(config);
  }

  @Override
  public BlockIOExecutor getBlockReader() throws IOException {
    S3BlockReaderConfig config = S3BlockReaderConfig.builder()
                                                    .bucket(_bucket)
                                                    .consistentAmazonS3(_consistentAmazonS3)
                                                    .objectPrefix(_objectPrefix)
                                                    .metricsFactory(_metricsFactory)
                                                    .build();
    return new S3BlockReader(config);
  }
}
