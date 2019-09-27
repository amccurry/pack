package pack.iscsi.server;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;
import pack.iscsi.block.LocalBlockStateStore;
import pack.iscsi.block.LocalBlockStateStoreConfig;
import pack.iscsi.io.IOUtils;
import pack.iscsi.manager.BaseTargetManager;
import pack.iscsi.manager.TargetManager;
import pack.iscsi.s3.S3TestProperties;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory.S3ExternalBlockStoreFactoryConfig;
import pack.iscsi.s3.block.S3GenerationBlockStore;
import pack.iscsi.s3.block.S3GenerationBlockStore.S3GenerationBlockStoreConfig;
import pack.iscsi.s3.volume.S3VolumeStore;
import pack.iscsi.s3.volume.S3VolumeStoreConfig;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.volume.BlockStorageModuleFactory;
import pack.iscsi.volume.BlockStorageModuleFactoryConfig;
import pack.iscsi.wal.WalTestProperties;
import pack.iscsi.wal.remote.RemoteWALClient;
import pack.iscsi.wal.remote.RemoteWALClient.RemoteWALClientConfig;
import pack.iscsi.wal.remote.RemoteWALServer;
import pack.iscsi.wal.remote.RemoteWALServer.RemoteWriteAheadLogServerConfig;

public class IscsiMiniCluster implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiMiniCluster.class);

  private static final String WAL = "wal";
  private static final String CACHE = "cache";

  private final IscsiServer _iscsiServer;
  private final Closer _closer;

  public static void main(String[] args) throws Exception {
    IscsiMiniClusterConfig config = IscsiMiniClusterConfig.builder()
                                                          .addresses(ImmutableSet.of("127.0.0.127"))
                                                          .port(3260)
                                                          .storageDir(new File("./test"))
                                                          .build();

    try (IscsiMiniCluster miniCluster = new IscsiMiniCluster(config)) {
      miniCluster.start();
      miniCluster.join();
    }
  }

  @Value
  @Builder(toBuilder = true)
  public static class InternalIscsiMiniClusterConfig {

    IscsiMiniClusterConfig config;

    String bucket;

    ConsistentAmazonS3 consistentAmazonS3;

    String objectPrefix;

    CuratorFramework curatorFramework;

    String zkWalPrefix;

  }

  public IscsiMiniCluster(IscsiMiniClusterConfig iscsiMiniClusterConfig) throws Exception {
    _closer = Closer.create();

    InternalIscsiMiniClusterConfig internalConfig = getInternalIscsiMiniClusterConfig(iscsiMiniClusterConfig);

    startWalServers(internalConfig, _closer);

    File storageDir = iscsiMiniClusterConfig.getStorageDir();
    File blockDataDir = new File(storageDir, CACHE);

    long maxCacheSizeInBytes = iscsiMiniClusterConfig.getMaxCacheSizeInBytes();

    BlockStateStore blockStateStore = getBlockStateStore(internalConfig);
    BlockGenerationStore blockStore = getBlockGenerationStore(internalConfig);
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory(internalConfig);

    MetricsFactory metricsFactory = getMetricsFactory(internalConfig);
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog(internalConfig);

    PackVolumeStore packVolumeStore = getPackVolumeStore(internalConfig);
    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .blockDataDir(blockDataDir)
                                                                            .blockStateStore(blockStateStore)
                                                                            .blockStore(blockStore)
                                                                            .externalBlockStoreFactory(
                                                                                externalBlockStoreFactory)
                                                                            .maxCacheSizeInBytes(maxCacheSizeInBytes)
                                                                            .metricsFactory(metricsFactory)
                                                                            .packVolumeStore(packVolumeStore)
                                                                            .writeAheadLog(writeAheadLog)
                                                                            .build();

    BlockStorageModuleFactory blockStorageModuleFactory = _closer.register(new BlockStorageModuleFactory(config));

    TargetManager targetManager = new BaseTargetManager(Arrays.asList(blockStorageModuleFactory));
    IscsiServerConfig iscsiServerConfig = IscsiServerConfig.builder()
                                                           .addresses(iscsiMiniClusterConfig.getAddresses())
                                                           .port(iscsiMiniClusterConfig.getPort())
                                                           .iscsiTargetManager(targetManager)
                                                           .build();
    _iscsiServer = new IscsiServer(iscsiServerConfig);
  }

  private PackVolumeStore getPackVolumeStore(InternalIscsiMiniClusterConfig internalConfig) {
    String bucket = internalConfig.getBucket();
    ConsistentAmazonS3 consistentAmazonS3 = internalConfig.getConsistentAmazonS3();
    String objectPrefix = internalConfig.getObjectPrefix();
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(bucket)
                                                    .consistentAmazonS3(consistentAmazonS3)
                                                    .objectPrefix(objectPrefix)
                                                    .build();
    return new S3VolumeStore(config);
  }

  private void startWalServers(InternalIscsiMiniClusterConfig internalConfig, Closer closer) throws Exception {
    String zkPrefix = internalConfig.getZkWalPrefix();
    CuratorFramework curatorFramework = internalConfig.getCuratorFramework();
    File walLogDir = new File(internalConfig.getConfig()
                                            .getStorageDir(),
        WAL);

    int serverCount = internalConfig.getConfig()
                                    .getWalServerCount();
    for (int i = 0; i < serverCount; i++) {
      RemoteWriteAheadLogServerConfig config = RemoteWriteAheadLogServerConfig.builder()
                                                                              .port(0)
                                                                              .zkPrefix(zkPrefix)
                                                                              .curatorFramework(curatorFramework)
                                                                              .walLogDir(new File(walLogDir,
                                                                                  Integer.toString(i)))
                                                                              .build();
      RemoteWALServer server = closer.register(new RemoteWALServer(config));
      server.start(false);
    }
  }

  private InternalIscsiMiniClusterConfig getInternalIscsiMiniClusterConfig(
      IscsiMiniClusterConfig iscsiMiniClusterConfig) throws Exception {
    String bucket = S3TestProperties.getBucket();
    ConsistentAmazonS3 consistentAmazonS3 = S3TestSetup.getConsistentAmazonS3();
    CuratorFramework curatorFramework = S3TestSetup.getCuratorFramework();
    String objectPrefix = S3TestProperties.getObjectPrefix();
    String zkWalPrefix = WalTestProperties.getPrefix();
    return InternalIscsiMiniClusterConfig.builder()
                                         .config(iscsiMiniClusterConfig)
                                         .bucket(bucket)
                                         .consistentAmazonS3(consistentAmazonS3)
                                         .curatorFramework(curatorFramework)
                                         .objectPrefix(objectPrefix)
                                         .zkWalPrefix(zkWalPrefix)
                                         .build();
  }

  private BlockWriteAheadLog getBlockWriteAheadLog(InternalIscsiMiniClusterConfig internalConfig) {
    CuratorFramework curatorFramework = internalConfig.getCuratorFramework();
    String zkWalPrefix = internalConfig.getZkWalPrefix();
    RemoteWALClientConfig config = RemoteWALClientConfig.builder()
                                                        .curatorFramework(curatorFramework)
                                                        .zkPrefix(zkWalPrefix)
                                                        .build();
    return new RemoteWALClient(config);
  }

  private MetricsFactory getMetricsFactory(InternalIscsiMiniClusterConfig internalConfig) {
    return MetricsFactory.NO_OP;
  }

  private BlockIOFactory getBlockIOFactory(InternalIscsiMiniClusterConfig internalConfig) {
    String bucket = internalConfig.getBucket();
    ConsistentAmazonS3 consistentAmazonS3 = internalConfig.getConsistentAmazonS3();
    String objectPrefix = internalConfig.getObjectPrefix();
    S3ExternalBlockStoreFactoryConfig config = S3ExternalBlockStoreFactoryConfig.builder()
                                                                                .bucket(bucket)
                                                                                .consistentAmazonS3(consistentAmazonS3)
                                                                                .objectPrefix(objectPrefix)
                                                                                .build();
    return new S3ExternalBlockStoreFactory(config);
  }

  private BlockGenerationStore getBlockGenerationStore(InternalIscsiMiniClusterConfig internalConfig) {
    String bucket = internalConfig.getBucket();
    ConsistentAmazonS3 consistentAmazonS3 = internalConfig.getConsistentAmazonS3();
    String objectPrefix = internalConfig.getObjectPrefix();
    S3GenerationBlockStoreConfig config = S3GenerationBlockStoreConfig.builder()
                                                                      .bucket(bucket)
                                                                      .consistentAmazonS3(consistentAmazonS3)
                                                                      .objectPrefix(objectPrefix)
                                                                      .build();
    return new S3GenerationBlockStore(config);
  }

  private BlockStateStore getBlockStateStore(InternalIscsiMiniClusterConfig internalConfig) {
    File storageDir = internalConfig.getConfig()
                                    .getStorageDir();
    LocalBlockStateStoreConfig config = LocalBlockStateStoreConfig.builder()
                                                                  .blockStateDir(new File(storageDir, "state"))
                                                                  .build();
    return new LocalBlockStateStore(config);
  }

  public void start() {
    _iscsiServer.start();
  }

  public void join() throws InterruptedException, ExecutionException {
    _iscsiServer.join();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, _closer, _iscsiServer);
  }

}
