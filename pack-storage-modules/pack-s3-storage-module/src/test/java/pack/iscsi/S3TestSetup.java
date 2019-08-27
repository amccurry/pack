package pack.iscsi;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import consistent.s3.ConsistentAmazonS3;
import consistent.s3.ConsistentAmazonS3Config;

public class S3TestSetup {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3TestSetup.class);

  private static final AtomicBoolean SETUP = new AtomicBoolean(false);
  private static AmazonS3 S3_CLIENT;
  private static CuratorFramework CURATOR_FRAMEWORK;
  private static ConsistentAmazonS3 CONSISTENT_AMAZON_S3;

  public synchronized static void setup() throws Exception {
    if (!isSetup()) {
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
      SETUP.set(true);
    }
  }

  public static AmazonS3 getAmazonS3() throws Exception {
    setup();
    return S3_CLIENT;
  }

  public static CuratorFramework getCuratorFramework() throws Exception {
    setup();
    return CURATOR_FRAMEWORK;
  }

  public static ConsistentAmazonS3 getConsistentAmazonS3() throws Exception {
    setup();
    return CONSISTENT_AMAZON_S3;
  }

  private static boolean isSetup() {
    return SETUP.get();
  }

}
