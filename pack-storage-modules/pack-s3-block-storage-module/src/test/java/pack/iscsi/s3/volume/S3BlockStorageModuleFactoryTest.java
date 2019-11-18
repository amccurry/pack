package pack.iscsi.s3.volume;

import java.io.File;

import org.junit.Before;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.block.LocalBlockStateStore;
import pack.iscsi.block.LocalBlockStateStoreConfig;
import pack.iscsi.io.IOUtils;
import pack.iscsi.s3.S3TestProperties;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory.S3ExternalBlockStoreFactoryConfig;
import pack.iscsi.s3.block.S3GenerationBlockStore;
import pack.iscsi.s3.block.S3GenerationBlockStore.S3GenerationBlockStoreConfig;
import pack.iscsi.spi.block.BlockCacheMetadataStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.volume.BlockStorageModuleFactoryTest;

public class S3BlockStorageModuleFactoryTest extends BlockStorageModuleFactoryTest {

  public static final File WAL_DATA_DIR = new File("./target/tmp/S3BlockStorageModuleFactoryTest/wal");
  public static final File[] BLOCK_DATA_DIRS = new File[] {
      new File("./target/tmp/S3BlockStorageModuleFactoryTest/block0"),
      new File("./target/tmp/S3BlockStorageModuleFactoryTest/block1") };
  public static final File BLOCK_STATE_DIR = new File("./target/tmp/S3BlockStorageModuleFactoryTest/state");
  private ConsistentAmazonS3 _consistentAmazonS3;
  private String _bucket;
  private String _objectPrefix;

  @Before
  public void setup() throws Exception {
    super.setup();
    _consistentAmazonS3 = S3TestSetup.getConsistentAmazonS3();
    _bucket = S3TestProperties.getBucket();
    _objectPrefix = S3TestProperties.getObjectPrefix();
    S3TestSetup.cleanS3(_bucket, _objectPrefix);
  }

  @Override
  protected void clearBlockData() {
    IOUtils.rmr(BLOCK_DATA_DIRS);
  }

  @Override
  protected void clearWalData() {
    IOUtils.rmr(WAL_DATA_DIR);
  }

  @Override
  protected void clearStateData() {
    IOUtils.rmr(BLOCK_STATE_DIR);
  }

  @Override
  protected File[] getBlockDataDirs() {
    return BLOCK_DATA_DIRS;
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

  @Override
  protected BlockCacheMetadataStore getBlockCacheMetadataStore() throws Exception {
    return new BlockCacheMetadataStore() {
    };
  }

}
