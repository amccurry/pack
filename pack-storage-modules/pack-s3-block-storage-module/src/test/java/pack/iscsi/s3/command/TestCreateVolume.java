package pack.iscsi.s3.command;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.volume.S3VolumeStore;
import pack.iscsi.s3.volume.S3VolumeStoreConfig;

public class TestCreateVolume {

  public static void main(String[] args) throws Exception {
    String bucket = args[0];
    ConsistentAmazonS3 consistentAmazonS3 = S3TestSetup.getConsistentAmazonS3();
    String objectPrefix = args[1];
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(bucket)
                                                    .consistentAmazonS3(consistentAmazonS3)
                                                    .objectPrefix(objectPrefix)
                                                    .build();
    try (S3VolumeStore volumeStore = new S3VolumeStore(config)) {
      volumeStore.createVolume("testvolume", 64 * 1024 * 1024, 10L * 1024L * 1024L * 1024L);
    }
  }

}
