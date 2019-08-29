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
import pack.iscsi.TestProperties;
import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.external.local.LocalBlockStore;
import pack.iscsi.external.local.LocalBlockWriteAheadLog;
import pack.iscsi.external.local.LocalExternalBlockStoreFactory;
import pack.iscsi.external.s3.S3ExternalBlockStoreFactory;
import pack.iscsi.manager.BaseTargetManager;
import pack.iscsi.manager.TargetManager;
import pack.iscsi.partitioned.storagemanager.BlockStorageModuleFactory;
import pack.iscsi.partitioned.storagemanager.BlockStorageModuleFactoryConfig;
import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
import pack.iscsi.spi.StorageModuleFactory;

public class IscsiServerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerMain.class);
  private static final boolean LOCAL = false;

  public static void main(String[] args) throws Exception {
    Set<String> addresses = new HashSet<String>();
    addresses.add("127.0.0.3");
    List<StorageModuleFactory> factories = new ArrayList<>();

    // factories.add(FileStorageModule.createFactory(new File("./volume")));

    try (BlockStorageModuleFactory factory = new BlockStorageModuleFactory(getConfig())) {
      factories.add(factory);

      TargetManager targetManager = new BaseTargetManager(factories);
      IscsiServerConfig config = IscsiServerConfig.builder()
                                                  .addresses(addresses)
                                                  .port(3260)
                                                  .iscsiTargetManager(targetManager)
                                                  .build();
      runServer(config);
    }
  }

  private static BlockStorageModuleFactoryConfig getConfig() throws Exception {
    File blockDataDir = new File("./iscsi-test/block-cache");
    BlockStore blockStore = new LocalBlockStore(new File("./iscsi-test/block-store"));
    ExternalBlockIOFactory externalBlockStoreFactory;
    if (LOCAL) {
      externalBlockStoreFactory = new LocalExternalBlockStoreFactory(new File("./iscsi-test/external-block"));
    } else {
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
      externalBlockStoreFactory = new S3ExternalBlockStoreFactory(consistentAmazonS3, TestProperties.getBucket(),
          TestProperties.getObjectPrefix());
    }
    long maxCacheSizeInBytes = 1_000_000_000L;
    BlockWriteAheadLog writeAheadLog = new LocalBlockWriteAheadLog(new File("./iscsi-test/wal"));

    return BlockStorageModuleFactoryConfig.builder()
                                          .blockDataDir(blockDataDir)
                                          .blockStore(blockStore)
                                          .externalBlockStoreFactory(externalBlockStoreFactory)
                                          .maxCacheSizeInBytes(maxCacheSizeInBytes)
                                          .writeAheadLog(writeAheadLog)
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
