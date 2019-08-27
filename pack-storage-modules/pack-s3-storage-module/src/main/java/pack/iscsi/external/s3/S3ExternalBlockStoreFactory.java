package pack.iscsi.external.s3;

import java.io.IOException;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.partitioned.block.BlockIOExecutor;

public class S3ExternalBlockStoreFactory implements ExternalBlockIOFactory {

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;

  public S3ExternalBlockStoreFactory(ConsistentAmazonS3 consistentAmazonS3, String bucket, String objectPrefix) {
    _consistentAmazonS3 = consistentAmazonS3;
    _bucket = bucket;
    _objectPrefix = objectPrefix;
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
