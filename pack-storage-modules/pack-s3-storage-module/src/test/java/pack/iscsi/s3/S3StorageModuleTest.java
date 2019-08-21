package pack.iscsi.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import consistent.s3.ConsistentAmazonS3;
import consistent.s3.ConsistentAmazonS3Config;
import pack.iscsi.s3.S3StorageModule.S3StorageModuleFactory;

public class S3StorageModuleTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageModuleTest.class);

  @Test
  public void testS3StorageModule() throws Exception {
    File volumesDir = new File("./target/tmp/S3StorageModuleTest/volume");
    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();

    RetryPolicy retryPolicy = new RetryForever((int) TimeUnit.SECONDS.toMillis(10));
    CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(TestProperties.getZooKeeperConnection(),
        retryPolicy);
    curatorFramework.getUnhandledErrorListenable()
                    .addListener((message, e) -> {
                      LOGGER.error("Unknown error " + message, e);
                    });
    curatorFramework.getConnectionStateListenable()
                    .addListener((c, newState) -> {
                      LOGGER.info("Connection state {}", newState);
                    });
    curatorFramework.start();

    ConsistentAmazonS3 consistentAmazonS3 = ConsistentAmazonS3.create(client, curatorFramework,
        ConsistentAmazonS3Config.builder()
                                .zkPrefix("/test")
                                .build());

    long cacheSizeInBytes = 200_000_000;

    String s3Bucket = TestProperties.getBucket();

    String s3ObjectPrefix = TestProperties.getObjectPrefix();

    S3StorageModuleFactoryConfig config = S3StorageModuleFactoryConfig.builder()
                                                                      .cacheSizeInBytes(cacheSizeInBytes)
                                                                      .consistentAmazonS3(consistentAmazonS3)
                                                                      .s3Bucket(s3Bucket)
                                                                      .s3ObjectPrefix(s3ObjectPrefix)
                                                                      .volumesDir(volumesDir)
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

}
