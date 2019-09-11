package pack.iscsi.s3.block;

import java.io.IOException;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.volume.BlockIOFactory;

public class S3ExternalBlockStoreFactory implements BlockIOFactory {

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;

  public S3ExternalBlockStoreFactory(S3ExternalBlockStoreFactoryConfig config) {
    _consistentAmazonS3 = config.getConsistentAmazonS3();
    _bucket = config.getBucket();
    _objectPrefix = config.getObjectPrefix();
  }

  @Override
  public BlockIOExecutor getBlockWriter() throws IOException {
    return new S3BlockWriter(_consistentAmazonS3, _bucket, _objectPrefix);
  }

  @Override
  public BlockIOExecutor getBlockReader() throws IOException {
    return new S3BlockReader(_consistentAmazonS3, _bucket, _objectPrefix);
  }
}
