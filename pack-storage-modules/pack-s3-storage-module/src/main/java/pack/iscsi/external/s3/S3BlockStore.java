package pack.iscsi.external.s3;

import java.io.IOException;
import java.util.List;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.partitioned.storagemanager.BlockStore;

public class S3BlockStore implements BlockStore {

  public static final long DEFAULT_STARTING_VOLUME_ID = 100_000_000;
  
  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;

  public S3BlockStore(ConsistentAmazonS3 consistentAmazonS3, String bucket, String objectPrefix) {
    _consistentAmazonS3 = consistentAmazonS3;
    _bucket = bucket;
    _objectPrefix = objectPrefix;
  }

  @Override
  public List<String> getVolumeNames() {
    throw new RuntimeException("not impl");
  }

  @Override
  public long getVolumeId(String name) {
    throw new RuntimeException("not impl");
  }

  @Override
  public int getBlockSize(long volumeId) {
    throw new RuntimeException("not impl");
  }

  @Override
  public long getLengthInBytes(long volumeId) {
    throw new RuntimeException("not impl");
  }

  @Override
  public long createVolume(String name, int blockSize, long lengthInBytes) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void destroyVolume(long volumeId) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void renameVolume(long volumeId, String name) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void growVolume(long volumeId, long lengthInBytes) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public long getLastStoreGeneration(long volumeId, long blockId) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException {
    throw new RuntimeException("not impl");
  }

}
