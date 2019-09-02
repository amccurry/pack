package pack.iscsi.external.s3;

import java.io.IOException;
import java.util.List;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.storagemanager.BlockStore;

public class S3BlockStore implements BlockStore {

  public static final long DEFAULT_STARTING_VOLUME_ID = 100_000_000;

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;
  private final Block _block;

  public S3BlockStore(ConsistentAmazonS3 consistentAmazonS3, String bucket, String objectPrefix, Block block) {
    _consistentAmazonS3 = consistentAmazonS3;
    _bucket = bucket;
    _objectPrefix = objectPrefix;
    _block = block;
  }

  @Override
  public long getLastStoreGeneration(long volumeId, long blockId) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException {
    // TODO Auto-generated method stub

  }

}
