package pack.iscsi.server.module;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.base.Splitter;

import consistent.s3.ConsistentAmazonS3;
import consistent.s3.ConsistentAmazonS3Config;
import pack.admin.PackVolumeAdminServer;
import pack.iscsi.block.LocalBlockStateStore;
import pack.iscsi.block.LocalBlockStateStoreConfig;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory.S3ExternalBlockStoreFactoryConfig;
import pack.iscsi.s3.block.S3GenerationBlockStore;
import pack.iscsi.s3.block.S3GenerationBlockStore.S3GenerationBlockStoreConfig;
import pack.iscsi.s3.volume.S3VolumeStore;
import pack.iscsi.s3.volume.S3VolumeStoreConfig;
import pack.iscsi.server.admin.AllVolumeTable;
import pack.iscsi.server.admin.AttachedVolumeTable;
import pack.iscsi.server.admin.CreateVolume;
import pack.iscsi.server.admin.GrowVolume;
import pack.iscsi.server.admin.MetricsTable;
import pack.iscsi.server.admin.VolumeInfoPage;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.StorageModuleFactory;
import pack.iscsi.spi.StorageModuleLoader;
import pack.iscsi.spi.block.BlockCacheMetadataStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.iscsi.volume.BlockStorageModuleFactory;
import pack.iscsi.volume.BlockStorageModuleFactoryConfig;
import spark.Service;
import swa.SWABuilder;
import swa.spi.Table;

public class S3StorageModuleLoader extends StorageModuleLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageModuleLoader.class);

  private static final String BLOCK_CACHE_SIZE_IN_BYTES = "block.cache.size.in.bytes";
  private static final String BLOCK_CACHE_DIR = "block.cache.dir";
  private static final String BLOCK_STATE_DIR = "block.state.dir";
  private static final String ZK_CONNECTION = "zk.connection";
  private static final String CONSISTENT_ZK_PREFIX = "consistent.zk.prefix";
  private static final String OBJECTPREFIX = "objectprefix";
  private static final String BUCKET = "bucket";

  public S3StorageModuleLoader() {
    super("s3");
  }

  @Override
  public StorageModuleFactory create(String name, Properties properties) throws IOException {
    BlockStorageModuleFactoryConfig config = getConfig(name, properties);
    return new BlockStorageModuleFactory(config);
  }

  private static BlockStorageModuleFactoryConfig getConfig(String instanceName, Properties properties)
      throws IOException {
    ConsistentAmazonS3 consistentAmazonS3 = getConsistentAmazonS3(properties, instanceName);

    PackVolumeStore volumeStore = getS3VolumeStore(properties, instanceName, consistentAmazonS3);

    MetricsTable metricsTable = new MetricsTable(30, 10, TimeUnit.SECONDS, volumeStore);

    File[] blockDataDirs = getBlockDataDirs(instanceName, properties);

    long maxCacheSizeInBytes = Long.parseLong(getPropertyNotNull(properties, BLOCK_CACHE_SIZE_IN_BYTES, instanceName));

    Service service = getSparkServiceIfNeeded(properties, instanceName);
    if (service != null) {
      Table allVolumeActionTable = new AllVolumeTable(volumeStore);
      Table attachedVolumeActionTable = new AttachedVolumeTable(volumeStore);
      VolumeInfoPage volumePage = new VolumeInfoPage(volumeStore, metricsTable);

      CreateVolume createVolume = new CreateVolume(volumeStore);
      GrowVolume growVolume = new GrowVolume(volumeStore);
      PackVolumeAdminServer adminServer = new PackVolumeAdminServer(service, volumeStore);

      SWABuilder.create(service)
                .startMenu()
                .addHtml(attachedVolumeActionTable)
                .addHtml(allVolumeActionTable)
                .addHtml(createVolume)
                .stopMenu()
                .startMenu("Metrics")
                .addHtml(metricsTable)
                .stopMenu()
                .addHtml(growVolume)
                .addHtml(volumePage)
                .setApplicationName("Pack")
                .setLogger(LOGGER)
                .build(attachedVolumeActionTable.getLinkName());

      adminServer.setup();
    }

    BlockCacheMetadataStore blockCacheMetadataStore = getBlockCacheMetadataStore(properties, instanceName,
        consistentAmazonS3, volumeStore);
    BlockGenerationStore blockStore = getS3BlockStore(properties, instanceName, consistentAmazonS3);
    BlockIOFactory externalBlockStoreFactory = getS3ExternalBlockStoreFactory(properties, instanceName,
        consistentAmazonS3, metricsTable);
    BlockStateStore blockStateStore = getBlockStateStore(properties, instanceName);

    return BlockStorageModuleFactoryConfig.builder()
                                          .packVolumeStore(volumeStore)
                                          .blockCacheMetadataStore(blockCacheMetadataStore)
                                          .blockStateStore(blockStateStore)
                                          .blockDataDirs(blockDataDirs)
                                          .blockStore(blockStore)
                                          .externalBlockStoreFactory(externalBlockStoreFactory)
                                          .maxCacheSizeInBytes(maxCacheSizeInBytes)
                                          .metricsFactory(metricsTable)
                                          .build();
  }

  private static File[] getBlockDataDirs(String instanceName, Properties properties) {
    String blockDataDirStr = getPropertyNotNull(properties, BLOCK_CACHE_DIR, instanceName);

    List<String> list = Splitter.on(',')
                                .splitToList(blockDataDirStr);

    File[] blockDataDirs = new File[list.size()];

    for (int i = 0; i < blockDataDirs.length; i++) {
      blockDataDirs[i] = new File(list.get(i));
    }
    return blockDataDirs;
  }

  private static BlockGenerationStore getS3BlockStore(Properties properties, String instanceName,
      ConsistentAmazonS3 consistentAmazonS3) {
    String bucket = getPropertyNotNull(properties, BUCKET, instanceName);
    String objectPrefix = getPropertyNotNull(properties, OBJECTPREFIX, instanceName);
    S3GenerationBlockStoreConfig config = S3GenerationBlockStoreConfig.builder()
                                                                      .bucket(bucket)
                                                                      .consistentAmazonS3(consistentAmazonS3)
                                                                      .objectPrefix(objectPrefix)
                                                                      .build();
    return new S3GenerationBlockStore(config);
  }

  private static PackVolumeStore getS3VolumeStore(Properties properties, String instanceName,
      ConsistentAmazonS3 consistentAmazonS3) {
    String bucket = getPropertyNotNull(properties, BUCKET, instanceName);
    String objectPrefix = getPropertyNotNull(properties, OBJECTPREFIX, instanceName);
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(bucket)
                                                    .consistentAmazonS3(consistentAmazonS3)
                                                    .objectPrefix(objectPrefix)
                                                    .build();
    return new S3VolumeStore(config);
  }

  private static BlockCacheMetadataStore getBlockCacheMetadataStore(Properties properties, String instanceName,
      ConsistentAmazonS3 consistentAmazonS3, PackVolumeStore volumeStore) {
    if (volumeStore instanceof BlockCacheMetadataStore) {
      return (BlockCacheMetadataStore) volumeStore;
    } else {
      return new BlockCacheMetadataStore() {
      };
    }
  }

  private static BlockStateStore getBlockStateStore(Properties properties, String instanceName) {
    File blockStateDir = new File(getPropertyNotNull(properties, BLOCK_STATE_DIR, instanceName));
    LocalBlockStateStoreConfig config = LocalBlockStateStoreConfig.builder()
                                                                  .blockStateDir(blockStateDir)
                                                                  .build();
    return new LocalBlockStateStore(config);
  }

  private static Service getSparkServiceIfNeeded(Properties properties, String instanceName) {
    return Service.ignite();
  }

  private static ConsistentAmazonS3 getConsistentAmazonS3(Properties properties, String instanceName)
      throws IOException {
    CuratorFramework curatorFramework = getCuratorFramework(properties, instanceName);
    String zkPrefix = getPropertyNotNull(properties, CONSISTENT_ZK_PREFIX, instanceName);
    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    try {
      return ConsistentAmazonS3.create(client, curatorFramework, ConsistentAmazonS3Config.DEFAULT.toBuilder()
                                                                                                 .zkPrefix(zkPrefix)
                                                                                                 .build());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static CuratorFramework getCuratorFramework(Properties properties, String instanceName) {
    String zkConnection = getPropertyNotNull(properties, ZK_CONNECTION, instanceName);
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
    Runtime.getRuntime()
           .addShutdownHook(new Thread(() -> curatorFramework.close()));
    return curatorFramework;
  }

  private static BlockIOFactory getS3ExternalBlockStoreFactory(Properties properties, String instanceName,
      ConsistentAmazonS3 consistentAmazonS3, MetricsFactory metricsFactory) throws IOException {
    String bucket = getPropertyNotNull(properties, BUCKET, instanceName);
    String objectPrefix = getPropertyNotNull(properties, OBJECTPREFIX, instanceName);

    S3ExternalBlockStoreFactoryConfig config = S3ExternalBlockStoreFactoryConfig.builder()
                                                                                .bucket(bucket)
                                                                                .consistentAmazonS3(consistentAmazonS3)
                                                                                .objectPrefix(objectPrefix)
                                                                                .metricsFactory(metricsFactory)
                                                                                .build();
    return new S3ExternalBlockStoreFactory(config);
  }

  private static String getPropertyNotNull(Properties properties, String name, String instanceName) {
    String property = properties.getProperty(name);
    if (property == null) {
      throw new IllegalArgumentException("Property " + name + " missing from configuration for " + instanceName);
    }
    return property;
  }
}
