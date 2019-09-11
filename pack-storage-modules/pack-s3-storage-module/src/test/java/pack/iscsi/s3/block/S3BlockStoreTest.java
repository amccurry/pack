package pack.iscsi.s3.block;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.TestProperties;
import pack.iscsi.s3.block.S3BlockStore;
import pack.iscsi.s3.block.S3BlockStoreConfig;
import pack.iscsi.s3.util.S3Utils;

public class S3BlockStoreTest {

  private static ConsistentAmazonS3 CONSISTENT_AMAZON_S3;
  private static String BUCKET;
  private static String OBJECT_PREFIX;

  @BeforeClass
  public static void setup() throws Exception {
    CONSISTENT_AMAZON_S3 = S3TestSetup.getConsistentAmazonS3();
    BUCKET = TestProperties.getBucket();
    OBJECT_PREFIX = TestProperties.getObjectPrefix();
    S3TestSetup.cleanS3(BUCKET, OBJECT_PREFIX);
  }

  @Test
  public void testS3BlockStore() throws Exception {
    long volumeId = 0;
    long blockId = 0;
    S3BlockStoreConfig config = S3BlockStoreConfig.builder()
                                                  .bucket(BUCKET)
                                                  .objectPrefix(OBJECT_PREFIX)
                                                  .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                  .build();

    try (S3BlockStore store = new S3BlockStore(config)) {
      store.setLastStoreGeneration(volumeId, blockId, 1);
      assertEquals(1, store.getLastStoreGeneration(volumeId, blockId));
    }
  }

  @Test
  public void testS3BlockStoreLoadExisting() throws Exception {
    long volumeId = 0;
    long blockId = 0;
    String key = S3Utils.getBlockGenerationKey(OBJECT_PREFIX, volumeId, blockId, 12345);
    CONSISTENT_AMAZON_S3.putObject(BUCKET, key, "test");

    // This may cause test to fail if s3 is too slow, need to update consistent
    // s3 to handle list objects
    Thread.sleep(TimeUnit.SECONDS.toMillis(3));

    S3BlockStoreConfig config = S3BlockStoreConfig.builder()
                                                  .bucket(BUCKET)
                                                  .objectPrefix(OBJECT_PREFIX)
                                                  .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                  .build();

    try (S3BlockStore store = new S3BlockStore(config)) {
      assertEquals(12345, store.getLastStoreGeneration(volumeId, blockId));
    }
  }

  @Test
  public void testS3BlockStoreLoadExistingWithMultipleBlocks() throws Exception {
    long volumeId = 0;
    long blockId1 = 1;
    long blockId2 = 12;
    int generation1 = 12345;
    int generation2 = 12346;
    {
      String key = S3Utils.getBlockGenerationKey(OBJECT_PREFIX, volumeId, blockId1, generation1);
      CONSISTENT_AMAZON_S3.putObject(BUCKET, key, "test");
    }
    {
      String key = S3Utils.getBlockGenerationKey(OBJECT_PREFIX, volumeId, blockId2, generation2);
      CONSISTENT_AMAZON_S3.putObject(BUCKET, key, "test");
    }
    
    // This may cause test to fail if s3 is too slow, need to update consistent
    // s3 to handle list objects
    Thread.sleep(TimeUnit.SECONDS.toMillis(3));

    S3BlockStoreConfig config = S3BlockStoreConfig.builder()
                                                  .bucket(BUCKET)
                                                  .objectPrefix(OBJECT_PREFIX)
                                                  .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                  .build();

    try (S3BlockStore store = new S3BlockStore(config)) {
      assertEquals(generation1, store.getLastStoreGeneration(volumeId, blockId1));
      assertEquals(generation2, store.getLastStoreGeneration(volumeId, blockId2));
    }
  }

}
