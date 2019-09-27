package pack.iscsi.server;

import java.io.File;

import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.block.LocalBlockStateStore;
import pack.iscsi.block.LocalBlockStateStoreConfig;
import pack.iscsi.io.IOUtils;
import pack.iscsi.s3.S3TestProperties;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory;
import pack.iscsi.s3.block.S3GenerationBlockStore;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory.S3ExternalBlockStoreFactoryConfig;
import pack.iscsi.s3.block.S3GenerationBlockStore.S3GenerationBlockStoreConfig;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.volume.BlockStorageModuleFactoryTest;
import pack.iscsi.wal.WalTestProperties;
import pack.iscsi.wal.WalTestSetup;
import pack.iscsi.wal.remote.RemoteWALClient;
import pack.iscsi.wal.remote.RemoteWALClient.RemoteWALClientConfig;
import pack.iscsi.wal.remote.RemoteWALServer;
import pack.iscsi.wal.remote.RemoteWALServer.RemoteWriteAheadLogServerConfig;

public class IntegrationBlockStorageModuleFactoryTest extends BlockStorageModuleFactoryTest {

  public static final File WAL_DATA_DIR = new File("./target/tmp/IntegrationBlockStorageModuleFactoryTest/wal");
  public static final File EXTERNAL_BLOCK_DATA_DIR = new File(
      "./target/tmp/IntegrationBlockStorageModuleFactoryTest/external");
  public static final File BLOCK_STATE_DIR = new File("./target/tmp/S3BlockStorageModuleFactoryTest/state");
  private ConsistentAmazonS3 _consistentAmazonS3;
  private String _bucket;
  private String _objectPrefix;
  private RemoteWALServer _walServer;

  @Before
  public void setup() throws Exception {
    super.setup();
    IOUtils.rmr(EXTERNAL_BLOCK_DATA_DIR);
    IOUtils.rmr(WAL_DATA_DIR);
    IOUtils.rmr(BLOCK_STATE_DIR);

    _consistentAmazonS3 = S3TestSetup.getConsistentAmazonS3();
    _bucket = S3TestProperties.getBucket();
    _objectPrefix = S3TestProperties.getObjectPrefix();
    S3TestSetup.cleanS3(_bucket, _objectPrefix);

    CuratorFramework curatorFramework = WalTestSetup.getCuratorFramework();

    RemoteWriteAheadLogServerConfig config = RemoteWriteAheadLogServerConfig.builder()
                                                                            .maxEntryPayload(1_000_000)
                                                                            .walLogDir(WAL_DATA_DIR)
                                                                            .port(0)
                                                                            .curatorFramework(curatorFramework)
                                                                            .zkPrefix(WalTestProperties.getPrefix())
                                                                            .build();
    _walServer = new RemoteWALServer(config);
    _walServer.start(false);
  }

  @After
  public void after() throws Exception {
    _walServer.stop();
  }

  @Override
  protected BlockIOFactory getBlockIOFactory() {
    S3ExternalBlockStoreFactoryConfig config = S3ExternalBlockStoreFactoryConfig.builder()
                                                                                .bucket(_bucket)
                                                                                .consistentAmazonS3(_consistentAmazonS3)
                                                                                .objectPrefix(_objectPrefix)
                                                                                .build();
    return new S3ExternalBlockStoreFactory(config);
  }

  @Override
  protected BlockWriteAheadLog getBlockWriteAheadLog() throws Exception {
    CuratorFramework curatorFramework = WalTestSetup.getCuratorFramework();
    RemoteWALClientConfig config = RemoteWALClientConfig.builder()
                                                                            .curatorFramework(curatorFramework)
                                                                            .zkPrefix(WalTestProperties.getPrefix())
                                                                            .build();
    return new RemoteWALClient(config);
  }

  @Override
  protected BlockGenerationStore getBlockGenerationStore() throws Exception {
    S3GenerationBlockStoreConfig config = S3GenerationBlockStoreConfig.builder()
                                                                      .bucket(_bucket)
                                                                      .consistentAmazonS3(_consistentAmazonS3)
                                                                      .objectPrefix(_objectPrefix)
                                                                      .build();
    return new S3GenerationBlockStore(config);
  }

  @Override
  protected BlockStateStore getBlockStateStore() {
    LocalBlockStateStoreConfig config = LocalBlockStateStoreConfig.builder()
                                                                  .blockStateDir(BLOCK_STATE_DIR)
                                                                  .build();
    return new LocalBlockStateStore(config);
  }

}
