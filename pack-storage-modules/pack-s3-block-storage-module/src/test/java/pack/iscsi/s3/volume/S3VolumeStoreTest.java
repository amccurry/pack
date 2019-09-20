package pack.iscsi.s3.volume;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.s3.S3TestProperties;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.spi.PackVolumeMetadata;

public class S3VolumeStoreTest {

  private static ConsistentAmazonS3 CONSISTENT_AMAZON_S3;
  private static String BUCKET;
  private static String OBJECT_PREFIX;

  @BeforeClass
  public static void setup() throws Exception {
    CONSISTENT_AMAZON_S3 = S3TestSetup.getConsistentAmazonS3();
    BUCKET = S3TestProperties.getBucket();
    OBJECT_PREFIX = S3TestProperties.getObjectPrefix();
    S3TestSetup.cleanS3(BUCKET, OBJECT_PREFIX);
  }

  @Test
  public void testS3VolumeStoreCreate() throws Exception {
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(BUCKET)
                                                    .objectPrefix(OBJECT_PREFIX)
                                                    .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                    .build();

    try (S3VolumeStore store = new S3VolumeStore(config)) {
      store.createVolume("test", 10_000, 100_000_000);
      PackVolumeMetadata volumeMetadata = store.getVolumeMetadata("test");
      assertEquals(10_000, volumeMetadata.getBlockSizeInBytes());
      assertEquals(100_000_000, volumeMetadata.getLengthInBytes());
    }
  }

  @Test
  public void testS3VolumeStoreList() throws Exception {
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(BUCKET)
                                                    .objectPrefix(OBJECT_PREFIX)
                                                    .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                    .build();

    try (S3VolumeStore store = new S3VolumeStore(config)) {
      store.createVolume("test1", 10_000, 100_000_000);
      store.createVolume("test2", 20_000, 200_000_000);

      List<String> list = new ArrayList<>(store.getAllVolumes());
      Collections.sort(list);
      assertEquals(Arrays.asList("test1", "test2"), list);
    }
  }

  @Test
  public void testS3VolumeStoreRename() throws Exception {
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(BUCKET)
                                                    .objectPrefix(OBJECT_PREFIX)
                                                    .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                    .build();

    try (S3VolumeStore store = new S3VolumeStore(config)) {
      store.createVolume("test3", 10_000, 100_000_000);
      long volumeId;
      {
        PackVolumeMetadata volumeMetadata = store.getVolumeMetadata("test3");
        assertEquals(10_000, volumeMetadata.getBlockSizeInBytes());
        assertEquals(100_000_000, volumeMetadata.getLengthInBytes());
        volumeId = volumeMetadata.getVolumeId();
      }
      store.renameVolume("test3", "test4");
      {
        PackVolumeMetadata volumeMetadata = store.getVolumeMetadata("test4");
        assertEquals(10_000, volumeMetadata.getBlockSizeInBytes());
        assertEquals(100_000_000, volumeMetadata.getLengthInBytes());
        assertEquals(volumeId, volumeMetadata.getVolumeId());
      }

    }
  }

}
