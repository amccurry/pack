package pack.iscsi.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
import pack.iscsi.manager.BaseTargetManager;
import pack.iscsi.manager.TargetManager;
import pack.iscsi.s3.TestProperties;
import pack.iscsi.s3.v1.S3StorageModule;
import pack.iscsi.s3.v1.S3StorageModuleFactoryConfig;
import pack.iscsi.spi.StorageModuleFactory;

public class IscsiServerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerMain.class);

  public static void main(String[] args) throws Exception {
    Set<String> addresses = new HashSet<String>();
    addresses.add("127.0.0.3");
    List<StorageModuleFactory> factories = new ArrayList<>();
    // factories.add(FileStorageModule.createFactory(new File("./volume")));

    S3StorageModuleFactoryConfig s3Config = getS3Config();
    factories.add(S3StorageModule.createFactory(s3Config));

    TargetManager targetManager = new BaseTargetManager(factories);
    IscsiServerConfig config = IscsiServerConfig.builder()
                                                .addresses(addresses)
                                                .port(3260)
                                                .iscsiTargetManager(targetManager)
                                                .build();
    runServer(config);
  }

  private static S3StorageModuleFactoryConfig getS3Config() throws Exception {
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
                                .zkPrefix("/test-run")
                                .build());
    return S3StorageModuleFactoryConfig.builder()
                                       .cacheSizeInBytes(10_000_000_000L)
                                       .consistentAmazonS3(consistentAmazonS3)
                                       .s3Bucket(TestProperties.getBucket())
                                       .s3ObjectPrefix(TestProperties.getObjectPrefix())
                                       .volumesDir(new File("./s3-volumes"))
                                       .build();
  }

  public static void runServer(IscsiServerConfig config) throws IOException, InterruptedException, ExecutionException {
    try (IscsiServer iscsiServer = new IscsiServer(config)) {
      LOGGER.info("Starting server");
      iscsiServer.start();
      LOGGER.info("Server started");
      iscsiServer.join();
    }
  }

}
