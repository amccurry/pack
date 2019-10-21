package pack.iscsi.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import consistent.s3.ConsistentAmazonS3;
import consistent.s3.ConsistentAmazonS3Config;
import pack.admin.PackVolumeAdminServer;
import pack.iscsi.block.LocalBlockStateStore;
import pack.iscsi.block.LocalBlockStateStoreConfig;
import pack.iscsi.file.block.storage.LocalExternalBlockStoreFactory;
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
import pack.iscsi.server.admin.MeterMetricsActionTable;
import pack.iscsi.server.admin.VolumePage;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.async.AsyncCompletableFuture;
import pack.iscsi.spi.block.BlockCacheMetadataStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.metric.Meter;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.iscsi.spi.wal.BlockJournalRange;
import pack.iscsi.spi.wal.BlockRecoveryWriter;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.volume.BlockStorageModuleFactoryConfig;
import pack.iscsi.wal.remote.RemoteWALClient;
import pack.iscsi.wal.remote.RemoteWALClient.RemoteWALClientConfig;
import spark.Service;
import swa.SWABuilder;
import swa.spi.Table;

public class IscsiConfigUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiConfigUtil.class);

  private static final String WAL_ZK_PREFIX = "wal.zk.prefix";
  private static final String EXTERNAL_BLOCK_LOCAL_DIR = "external.block.local.dir";
  private static final String EXTERNAL_BLOCK_TYPE = "external.block.type";
  private static final String BLOCK_CACHE_SIZE_IN_BYTES = "block.cache.size.in.bytes";
  private static final String BLOCK_CACHE_DIR = "block.cache.dir";
  private static final String BLOCK_STATE_DIR = "block.state.dir";
  private static final String ZK_CONNECTION = "zk.connection";
  private static final String CONSISTENT_S3_ZK_PREFIX = "consistent.s3.zk.prefix";
  private static final String S3_OBJECTPREFIX = "s3.objectprefix";
  private static final String S3_BUCKET = "s3.bucket";

  private static final Joiner JOINER_DOT = Joiner.on('.');

  public static List<BlockStorageModuleFactoryConfig> getConfigs(File file) throws Exception {
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

    MetricsFactory metricsFactory = getMetricsFactoryIfNeeded();

    File blockDataDir = new File(getPropertyNotNull(properties, BLOCK_CACHE_DIR, configFile));
    long maxCacheSizeInBytes = Long.parseLong(getPropertyNotNull(properties, BLOCK_CACHE_SIZE_IN_BYTES, configFile));

    PackVolumeStore volumeStore = getVolumeStore(properties, configFile, consistentAmazonS3);

    Service service = getSparkServiceIfNeeded(properties, configFile);
    if (service != null) {
      Table allVolumeActionTable = new AllVolumeTable(volumeStore);
      Table attachedVolumeActionTable = new AttachedVolumeTable(volumeStore);
      VolumePage volumePage = new VolumePage(volumeStore);
      MeterMetricsActionTable meterMetricsActionTable = new MeterMetricsActionTable(metricsFactory);

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
                .addHtml(meterMetricsActionTable)
                .stopMenu()
                .addHtml(growVolume)
                .addHtml(volumePage)
                .setApplicationName("Pack")
                .build(attachedVolumeActionTable.getLinkName());

      adminServer.setup();
    }

    BlockCacheMetadataStore blockCacheMetadataStore = getBlockCacheMetadataStore(properties, configFile,
        consistentAmazonS3, volumeStore);
    BlockGenerationStore blockStore = getBlockStore(properties, configFile, consistentAmazonS3);
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog(properties, configFile,
        consistentAmazonS3.getCuratorFramework());
    BlockIOFactory externalBlockStoreFactory = getExternalBlockIOFactory(properties, configFile, consistentAmazonS3);
    BlockStateStore blockStateStore = getBlockStateStore(properties, configFile);

    return BlockStorageModuleFactoryConfig.builder()
                                          .packVolumeStore(volumeStore)
                                          .blockCacheMetadataStore(blockCacheMetadataStore)
                                          .blockStateStore(blockStateStore)
                                          .blockDataDir(blockDataDir)
                                          .blockStore(blockStore)
                                          .externalBlockStoreFactory(externalBlockStoreFactory)
                                          .maxCacheSizeInBytes(maxCacheSizeInBytes)
                                          .writeAheadLog(writeAheadLog)
                                          .metricsFactory(metricsFactory)
                                          .build();
  }

  private static BlockCacheMetadataStore getBlockCacheMetadataStore(Properties properties, File configFile,
      ConsistentAmazonS3 consistentAmazonS3, PackVolumeStore volumeStore) {
    if (volumeStore instanceof BlockCacheMetadataStore) {
      return (BlockCacheMetadataStore) volumeStore;
    } else {
      return new BlockCacheMetadataStore() {
      };
    }
  }

  private static BlockStateStore getBlockStateStore(Properties properties, File configFile) {
    File blockStateDir = new File(getPropertyNotNull(properties, BLOCK_STATE_DIR, configFile));
    LocalBlockStateStoreConfig config = LocalBlockStateStoreConfig.builder()
                                                                  .blockStateDir(blockStateDir)
                                                                  .build();
    return new LocalBlockStateStore(config);
  }

  private static MetricsFactory getMetricsFactoryIfNeeded() {
    MetricRegistry metricRegistry = new MetricRegistry();
    return new MetricsFactory() {

      @Override
      public Object getMetricRegistry() {
        return metricRegistry;
      }

      @Override
      public Meter meter(Class<?> clazz, String... name) {
        com.codahale.metrics.Meter meter = metricRegistry.meter(getName(clazz, name));
        return count -> meter.mark(count);
      }
    };
  }

  private static String getName(Class<?> clazz, String... names) {
    return clazz.getSimpleName() + '.' + JOINER_DOT.join(names);
  }

  private static Service getSparkServiceIfNeeded(Properties properties, File configFile) {
    return Service.ignite();
  }

  private static BlockWriteAheadLog getBlockWriteAheadLog(Properties properties, File configFile,
      CuratorFramework curatorFramework) throws Exception {
    return noOpWAL();
    // return getRemoteBlockWriteAheadLog(properties, configFile,
    // curatorFramework);
  }

  private static BlockWriteAheadLog noOpWAL() {
    return new BlockWriteAheadLog() {

      @Override
      public AsyncCompletableFuture write(long volumeId, long blockId, long generation, long position, byte[] bytes,
          int offset, int len) throws IOException {
        return AsyncCompletableFuture.completedFuture();
      }

      @Override
      public void releaseJournals(long volumeId, long blockId, long generation) throws IOException {

      }

      @Override
      public long recoverFromJournal(BlockRecoveryWriter writer, BlockJournalRange range, long onDiskGeneration)
          throws IOException {
        return onDiskGeneration;
      }

      @Override
      public List<BlockJournalRange> getJournalRanges(long volumeId, long blockId, long onDiskGeneration,
          boolean closeExistingWriter) throws IOException {
        return Arrays.asList();
      }
    };
  }

  private static BlockWriteAheadLog getRemoteBlockWriteAheadLog(Properties properties, File configFile,
      CuratorFramework curatorFramework) throws Exception {
    String zkPrefix = getPropertyNotNull(properties, WAL_ZK_PREFIX, configFile);
    RemoteWALClientConfig config = RemoteWALClientConfig.builder()
                                                        .curatorFramework(curatorFramework)
                                                        .zkPrefix(zkPrefix)
                                                        .timeout(TimeUnit.MINUTES.toMillis(10))
                                                        .build();
    return new RemoteWALClient(config);
  }

  private static PackVolumeStore getVolumeStore(Properties properties, File configFile,
      ConsistentAmazonS3 consistentAmazonS3) {
    return getS3VolumeStore(properties, configFile, consistentAmazonS3);
  }

  private static PackVolumeStore getS3VolumeStore(Properties properties, File configFile,
      ConsistentAmazonS3 consistentAmazonS3) {
    String bucket = getPropertyNotNull(properties, S3_BUCKET, configFile);
    String objectPrefix = getPropertyNotNull(properties, S3_OBJECTPREFIX, configFile);
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(bucket)
                                                    .consistentAmazonS3(consistentAmazonS3)
                                                    .objectPrefix(objectPrefix)
                                                    .build();
    return new S3VolumeStore(config);
  }

  private static BlockGenerationStore getBlockStore(Properties properties, File configFile,
      ConsistentAmazonS3 consistentAmazonS3) {
    return getS3BlockStore(properties, configFile, consistentAmazonS3);
  }

  private static ConsistentAmazonS3 getConsistentAmazonS3IfNeeded(Properties properties, File configFile)
      throws Exception {
    CuratorFramework curatorFramework = getCuratorFrameworkIfNeeded(properties, configFile);
    if (curatorFramework == null) {
      return null;
    }
    String zkPrefix = getPropertyNotNull(properties, CONSISTENT_S3_ZK_PREFIX, configFile);
    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    ConsistentAmazonS3 consistentAmazonS3 = ConsistentAmazonS3.create(client, curatorFramework,
        ConsistentAmazonS3Config.DEFAULT.toBuilder()
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
    Runtime.getRuntime()
           .addShutdownHook(new Thread(() -> curatorFramework.close()));
    return curatorFramework;
  }

  private static BlockGenerationStore getS3BlockStore(Properties properties, File configFile,
      ConsistentAmazonS3 consistentAmazonS3) {
    String bucket = getPropertyNotNull(properties, S3_BUCKET, configFile);
    String objectPrefix = getPropertyNotNull(properties, S3_OBJECTPREFIX, configFile);
    S3GenerationBlockStoreConfig config = S3GenerationBlockStoreConfig.builder()
                                                                      .bucket(bucket)
                                                                      .consistentAmazonS3(consistentAmazonS3)
                                                                      .objectPrefix(objectPrefix)
                                                                      .build();
    return new S3GenerationBlockStore(config);
  }

  private static BlockIOFactory getExternalBlockIOFactory(Properties properties, File configFile,
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

  private static BlockIOFactory getS3ExternalBlockStoreFactory(Properties properties, File configFile,
      ConsistentAmazonS3 consistentAmazonS3) throws Exception {
    String bucket = getPropertyNotNull(properties, S3_BUCKET, configFile);
    String objectPrefix = getPropertyNotNull(properties, S3_OBJECTPREFIX, configFile);

    S3ExternalBlockStoreFactoryConfig config = S3ExternalBlockStoreFactoryConfig.builder()
                                                                                .bucket(bucket)
                                                                                .consistentAmazonS3(consistentAmazonS3)
                                                                                .objectPrefix(objectPrefix)
                                                                                .build();
    return new S3ExternalBlockStoreFactory(config);
  }

  // private static int getPropertyWithDefault(Properties properties, String
  // name, File file, int defaultValue) {
  // String property = properties.getProperty(name);
  // if (property == null) {
  // return defaultValue;
  // }
  // return Integer.parseInt(property);
  // }

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
