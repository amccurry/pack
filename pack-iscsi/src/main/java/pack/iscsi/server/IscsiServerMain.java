package pack.iscsi.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.external.local.LocalBlockWriteAheadLog;
import pack.iscsi.external.local.LocalExternalBlockStoreFactory;
import pack.iscsi.manager.BaseTargetManager;
import pack.iscsi.manager.TargetManager;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.storagemanager.BlockStorageModuleFactory;
import pack.iscsi.partitioned.storagemanager.BlockStorageModuleFactoryConfig;
import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
import pack.iscsi.spi.StorageModuleFactory;

public class IscsiServerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerMain.class);

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

  private static BlockStorageModuleFactoryConfig getConfig() throws IOException {
    File blockDataDir = new File("./iscsi-test/block-cache");
    BlockStore blockStore = getBlockStore();
    ExternalBlockIOFactory externalBlockStoreFactory = new LocalExternalBlockStoreFactory(
        new File("./iscsi-test/external-block"));
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

  private static BlockStore getBlockStore() {
    ConcurrentMap<Long, Long> gens = new ConcurrentHashMap<>();
    return new BlockStore() {

      @Override
      public long getLastStoreGeneration(long volumeId, long blockId) {
        Long gen = gens.get(blockId);
        if (gen == null) {
          return Block.MISSING_BLOCK_GENERATION;
        }
        return gen;
      }

      @Override
      public void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) {
        gens.put(blockId, lastStoredGeneration);
      }

      @Override
      public List<String> getVolumeNames() {
        return Arrays.asList("test");
      }

      @Override
      public long getVolumeId(String name) {
        return 0;
      }

      @Override
      public long getLengthInBytes(long volumeId) {
        return 10_000_000_000L;
      }

      @Override
      public int getBlockSize(long volumeId) {
        return 64 * 1024 * 1024;
      }
    };
  }

  // private static S3StorageModuleFactoryConfig getS3Config() throws Exception
  // {
  // AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
  // RetryPolicy retryPolicy = new RetryForever((int)
  // TimeUnit.SECONDS.toMillis(10));
  // CuratorFramework curatorFramework =
  // CuratorFrameworkFactory.newClient(TestProperties.getZooKeeperConnection(),
  // retryPolicy);
  // curatorFramework.getUnhandledErrorListenable()
  // .addListener((message, e) -> {
  // LOGGER.error("Unknown error " + message, e);
  // });
  // curatorFramework.getConnectionStateListenable()
  // .addListener((c, newState) -> {
  // LOGGER.info("Connection state {}", newState);
  // });
  // curatorFramework.start();
  // ConsistentAmazonS3 consistentAmazonS3 = ConsistentAmazonS3.create(client,
  // curatorFramework,
  // ConsistentAmazonS3Config.builder()
  // .zkPrefix("/test-run")
  // .build());
  // return S3StorageModuleFactoryConfig.builder()
  // .cacheSizeInBytes(10_000_000_000L)
  // .consistentAmazonS3(consistentAmazonS3)
  // .s3Bucket(TestProperties.getBucket())
  // .s3ObjectPrefix(TestProperties.getObjectPrefix())
  // .volumesDir(new File("./s3-volumes"))
  // .build();
  // }

  public static void runServer(IscsiServerConfig config) throws IOException, InterruptedException, ExecutionException {
    try (IscsiServer iscsiServer = new IscsiServer(config)) {
      LOGGER.info("Starting server");
      iscsiServer.start();
      LOGGER.info("Server started");
      iscsiServer.join();
    }
  }

}
