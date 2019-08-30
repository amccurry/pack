package pack.iscsi.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
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
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;

import consistent.s3.ConsistentAmazonS3;
import consistent.s3.ConsistentAmazonS3Config;
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

  private static final String EXTERNAL_BLOCK_S3_OBJECTPREFIX = "external.block.s3.objectprefix";
  private static final String EXTERNAL_BLOCK_S3_BUCKET = "external.block.s3.bucket";
  private static final String EXTERNAL_BLOCK_LOCAL_DIR = "external.block.local.dir";
  private static final String EXTERNAL_BLOCK_TYPE = "external.block.type";
  private static final String BLOCK_CACHE_SIZE_IN_BYTES = "block.cache.size.in.bytes";
  private static final String WAL_DIR = "wal.dir";
  private static final String BLOCK_STORE_DIR = "block.store.dir";
  private static final String BLOCK_CACHE_DIR = "block.cache.dir";
  private static final String ZK_CONNECTION = "zk.connection";
  private static final String ZK_PREFIX = "zk.prefix";

  public static void main(String[] args) throws Exception {
    Set<String> addresses = new HashSet<String>();
    addresses.add("127.0.0.3");
    try (Closer closer = Closer.create()) {
      List<StorageModuleFactory> factories = new ArrayList<>();
      List<BlockStorageModuleFactoryConfig> configs = getConfigs(new File("etc/pack/conf.d/"));
      for (BlockStorageModuleFactoryConfig config : configs) {
        factories.add(closer.register(new BlockStorageModuleFactory(config)));
      }
      TargetManager targetManager = new BaseTargetManager(factories);
      IscsiServerConfig config = IscsiServerConfig.builder()
                                                  .addresses(addresses)
                                                  .port(3260)
                                                  .iscsiTargetManager(targetManager)
                                                  .build();
      runServer(config);
    }
  }

  public static void runServer(IscsiServerConfig config) throws IOException, InterruptedException, ExecutionException {
    try (IscsiServer iscsiServer = new IscsiServer(config)) {
      LOGGER.info("Starting server");
      iscsiServer.start();
      LOGGER.info("Server started");
      iscsiServer.join();
    }
  }

  private static List<BlockStorageModuleFactoryConfig> getConfigs(File file) throws Exception {
    if (!file.exists()) {
      return ImmutableList.of();
    }
    if (file.isDirectory()) {
      List<BlockStorageModuleFactoryConfig> list = new ArrayList<>();
      for (File f : file.listFiles()) {
        list.addAll(getConfigs(f));
      }
      return ImmutableList.copyOf(list);
    } else if (file.isFile()) {
      Properties properties = new Properties();
      try (FileInputStream input = new FileInputStream(file)) {
        properties.load(input);
      }
      return ImmutableList.of(getConfig(properties, file));
    } else {
      return ImmutableList.of();
    }
  }

  private static BlockStorageModuleFactoryConfig getConfig(Properties properties, File configFile) throws Exception {
    ConsistentAmazonS3 consistentAmazonS3 = getConsistentAmazonS3IfNeeded(properties, configFile);

    File blockDataDir = new File(getPropertyNotNull(properties, BLOCK_CACHE_DIR, configFile));
    File walLogDir = new File(getPropertyNotNull(properties, WAL_DIR, configFile));
    long maxCacheSizeInBytes = Long.parseLong(getPropertyNotNull(properties, BLOCK_CACHE_SIZE_IN_BYTES, configFile));

    BlockStore blockStore = getBlockStore(properties, configFile, consistentAmazonS3);
    BlockWriteAheadLog writeAheadLog = new LocalBlockWriteAheadLog(walLogDir);
    ExternalBlockIOFactory externalBlockStoreFactory = getExternalBlockIOFactory(properties, configFile,
        consistentAmazonS3);
    return BlockStorageModuleFactoryConfig.builder()
                                          .blockDataDir(blockDataDir)
                                          .blockStore(blockStore)
                                          .externalBlockStoreFactory(externalBlockStoreFactory)
                                          .maxCacheSizeInBytes(maxCacheSizeInBytes)
                                          .writeAheadLog(writeAheadLog)
                                          .build();
  }

  private static ConsistentAmazonS3 getConsistentAmazonS3IfNeeded(Properties properties, File configFile)
      throws Exception {
    CuratorFramework curatorFramework = getCuratorFrameworkIfNeeded(properties, configFile);
    if (curatorFramework == null) {
      return null;
    }
    String zkPrefix = getPropertyNotNull(properties, ZK_PREFIX, configFile);
    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    ConsistentAmazonS3 consistentAmazonS3 = ConsistentAmazonS3.create(client, curatorFramework,
        ConsistentAmazonS3Config.builder()
                                .zkPrefix(zkPrefix)
                                .build());
    return consistentAmazonS3;
  }

  private static CuratorFramework getCuratorFrameworkIfNeeded(Properties properties, File configFile) {
    String zkConnection = getProperty(properties, ZK_CONNECTION);
    if (zkConnection == null) {
      return null;
    }

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

  private static BlockStore getBlockStore(Properties properties, File configFile,
      ConsistentAmazonS3 consistentAmazonS3) {
    String type = getPropertyNotNull(properties, "block.store.type", configFile);
    switch (type) {
    case "local":
      return getLocalBlockStore(properties, configFile);
    case "s3":
      return getS3BlockStore(properties, configFile, consistentAmazonS3);
    default:
      throw new IllegalArgumentException("external block type " + type + " unknown");
    }
  }

  private static BlockStore getS3BlockStore(Properties properties, File configFile,
      ConsistentAmazonS3 consistentAmazonS3) {
    throw new RuntimeException("Not implmented");
  }

  private static BlockStore getLocalBlockStore(Properties properties, File configFile) {
    File blockStoreDir = new File(getPropertyNotNull(properties, BLOCK_STORE_DIR, configFile));
    return new LocalBlockStore(blockStoreDir);
  }

  private static ExternalBlockIOFactory getExternalBlockIOFactory(Properties properties, File configFile,
      ConsistentAmazonS3 consistentAmazonS3) throws Exception {
    String type = getPropertyNotNull(properties, EXTERNAL_BLOCK_TYPE, configFile);
    switch (type) {
    case "local":
      return getLocalExternalBlockStoreFactory(properties, configFile);
    case "s3":
      return getS3ExternalBlockStoreFactory(properties, configFile, consistentAmazonS3);
    default:
      throw new IllegalArgumentException("external block type " + type + " unknown");
    }
  }

  private static LocalExternalBlockStoreFactory getLocalExternalBlockStoreFactory(Properties properties,
      File configFile) {
    File storeDir = new File(getPropertyNotNull(properties, EXTERNAL_BLOCK_LOCAL_DIR, configFile));
    return new LocalExternalBlockStoreFactory(storeDir);
  }

  private static ExternalBlockIOFactory getS3ExternalBlockStoreFactory(Properties properties, File configFile,
      ConsistentAmazonS3 consistentAmazonS3) throws Exception {
    String bucket = getPropertyNotNull(properties, EXTERNAL_BLOCK_S3_BUCKET, configFile);
    String objectPrefix = getPropertyNotNull(properties, EXTERNAL_BLOCK_S3_OBJECTPREFIX, configFile);
    return new S3ExternalBlockStoreFactory(consistentAmazonS3, bucket, objectPrefix);
  }

  private static String getPropertyNotNull(Properties properties, String name, File file) {
    String property = properties.getProperty(name);
    if (property == null) {
      throw new IllegalArgumentException("Property " + name + " missing from configuration file " + file);
    }
    return property;
  }

  private static String getProperty(Properties properties, String name) {
    return properties.getProperty(name);
  }

}
