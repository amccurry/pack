package pack.iscsi.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import consistent.s3.ConsistentAmazonS3;
import consistent.s3.ConsistentAmazonS3Config;
import pack.iscsi.s3.v1.S3StorageModule;
import pack.iscsi.s3.v1.S3StorageModuleFactoryConfig;
import pack.iscsi.s3.v1.S3StorageModule.S3StorageModuleFactory;
import pack.util.IOUtils;

public class S3StorageModuleTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageModuleTest.class);

  private static AmazonS3 S3_CLIENT;
  private static File VOLUME_DIR;
  private static CuratorFramework CURATOR_FRAMEWORK;
  private static int CACHE_IN_BYTES;
  private static String S3_BUCKET;
  private static String S3_OBJECT_PREFIX;
  private static ConsistentAmazonS3 CONSISTENT_AMAZON_S3;

  @BeforeClass
  public static void setup() throws Exception {
    VOLUME_DIR = new File("./target/tmp/S3StorageModuleTest/volume");
    IOUtils.rmr(VOLUME_DIR);
    S3_CLIENT = AmazonS3ClientBuilder.defaultClient();

    RetryPolicy retryPolicy = new RetryForever((int) TimeUnit.SECONDS.toMillis(10));
    CURATOR_FRAMEWORK = CuratorFrameworkFactory.newClient(TestProperties.getZooKeeperConnection(), retryPolicy);
    CURATOR_FRAMEWORK.getUnhandledErrorListenable()
                     .addListener((message, e) -> {
                       LOGGER.error("Unknown error " + message, e);
                     });
    CURATOR_FRAMEWORK.getConnectionStateListenable()
                     .addListener((c, newState) -> {
                       LOGGER.info("Connection state {}", newState);
                     });
    CURATOR_FRAMEWORK.start();

    ConsistentAmazonS3Config config = ConsistentAmazonS3Config.builder()
                                                              .zkPrefix("/s3/consistent/test")
                                                              .build();

    CONSISTENT_AMAZON_S3 = ConsistentAmazonS3.create(S3_CLIENT, CURATOR_FRAMEWORK, config);

    CACHE_IN_BYTES = 10_000_000;

    S3_BUCKET = TestProperties.getBucket();

    S3_OBJECT_PREFIX = TestProperties.getObjectPrefix();
  }

  @Test
  public void testS3StorageModuleSimple() throws Exception {

    S3StorageModuleFactoryConfig config = S3StorageModuleFactoryConfig.builder()
                                                                      .cacheSizeInBytes(CACHE_IN_BYTES)
                                                                      .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                                      .s3Bucket(S3_BUCKET)
                                                                      .s3ObjectPrefix(S3_OBJECT_PREFIX)
                                                                      .volumesDir(VOLUME_DIR)
                                                                      .build();

    S3StorageModuleFactory factory = S3StorageModule.createFactory(config);

    List<String> list = factory.getStorageModuleNames();
    assertEquals(1, list.size());

    try (S3StorageModule storageModule = factory.getStorageModule("test")) {
      byte[] buffer = new byte[3000];
      Arrays.fill(buffer, (byte) 3);
      storageModule.write(buffer, 12345);

      byte[] buffer2 = new byte[3000];
      storageModule.read(buffer2, 12345);

      assertTrue(Arrays.equals(buffer, buffer2));
    }
  }

  @Test
  public void testS3StorageModuleAdvanced() throws Exception {

    S3StorageModuleFactoryConfig config = S3StorageModuleFactoryConfig.builder()
                                                                      .cacheSizeInBytes(CACHE_IN_BYTES)
                                                                      .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                                      .s3Bucket(S3_BUCKET)
                                                                      .s3ObjectPrefix(S3_OBJECT_PREFIX)
                                                                      .volumesDir(VOLUME_DIR)
                                                                      .build();

    S3StorageModuleFactory factory = S3StorageModule.createFactory(config);

    List<String> list = factory.getStorageModuleNames();
    assertEquals(1, list.size());

    try (S3StorageModule storageModule = factory.getStorageModule("test")) {
      Random random = new Random();
      for (int i = 0; i < 1000; i++) {
        int bufferLength = random.nextInt(128_000);
        int position = random.nextInt(Integer.MAX_VALUE - bufferLength);
        byte[] buffer = new byte[bufferLength];
        storageModule.write(buffer, position);
        byte[] buffer2 = new byte[bufferLength];
        storageModule.read(buffer2, position);
        assertTrue(Arrays.equals(buffer, buffer2));
        Thread.sleep(1000);
        // Map<S3CacheKey, S3CacheValue> map = factory.getCacheAsMap();
        // for (Entry<S3CacheKey, S3CacheValue> entry : map.entrySet()) {
        // System.out.println(entry.getKey() + " " + entry.getValue()
        // .getBlockSize());
        // }
      }
    }
  }
}
