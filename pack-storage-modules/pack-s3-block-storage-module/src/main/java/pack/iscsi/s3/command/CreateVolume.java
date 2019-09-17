package pack.iscsi.s3.command;

import java.util.concurrent.TimeUnit;

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
import pack.iscsi.s3.volume.S3VolumeStore;
import pack.iscsi.s3.volume.S3VolumeStoreConfig;

public class CreateVolume {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateVolume.class);

  public static void main(String[] args) throws Exception {
    String bucket = args[0];
    String objectPrefix = args[1];
    ConsistentAmazonS3 consistentAmazonS3 = getConsistentAmazonS3(args[2], args[3]);
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(bucket)
                                                    .consistentAmazonS3(consistentAmazonS3)
                                                    .objectPrefix(objectPrefix)
                                                    .build();
    try (S3VolumeStore volumeStore = new S3VolumeStore(config)) {
      volumeStore.createVolume(args[4], 64 * 1024 * 1024, 100_000_000_000L);
    }
  }

  private static CuratorFramework getCuratorFramework(String zkConnection) {
    RetryPolicy retryPolicy = new RetryForever((int) TimeUnit.SECONDS.toMillis(10));
    CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zkConnection, retryPolicy);
    curatorFramework.getUnhandledErrorListenable()
                    .addListener((message, e) -> {
                      LOGGER.error("Unknown error " + message, e);
                    });
    curatorFramework.getConnectionStateListenable()
                    .addListener((c, newState) -> {
                      LOGGER.info("Connection state {}", newState);
                    });
    curatorFramework.start();
    return curatorFramework;
  }

  private static ConsistentAmazonS3 getConsistentAmazonS3(String zkConnection, String zkPrefix) throws Exception {
    CuratorFramework curatorFramework = getCuratorFramework(zkConnection);
    if (curatorFramework == null) {
      return null;
    }
    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    ConsistentAmazonS3 consistentAmazonS3 = ConsistentAmazonS3.create(client, curatorFramework,
        ConsistentAmazonS3Config.builder()
                                .zkPrefix(zkPrefix)
                                .build());
    return consistentAmazonS3;
  }
}
