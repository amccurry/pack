package pack.iscsi.s3.volume;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.io.IOUtils;
import pack.iscsi.s3.S3TestProperties;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory.S3ExternalBlockStoreFactoryConfig;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.volume.BlockStorageModuleFactoryTest;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog.LocalBlockWriteAheadLogConfig;

public class S3BlockStorageModuleFactoryTest extends BlockStorageModuleFactoryTest {

  public static final File WAL_DATA_DIR = new File("./target/tmp/S3BlockStorageModuleFactoryTest/wal");
  public static final File EXTERNAL_BLOCK_DATA_DIR = new File("./target/tmp/S3BlockStorageModuleFactoryTest/external");
  private ConsistentAmazonS3 _consistentAmazonS3;
  private String _bucket;
  private String _objectPrefix;

  @Before
  public void setup() throws Exception {
    super.setup();
    IOUtils.rmr(EXTERNAL_BLOCK_DATA_DIR);
    IOUtils.rmr(WAL_DATA_DIR);

    _consistentAmazonS3 = S3TestSetup.getConsistentAmazonS3();
    _bucket = S3TestProperties.getBucket();
    _objectPrefix = S3TestProperties.getObjectPrefix();
    S3TestSetup.cleanS3(_bucket, _objectPrefix);
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
    LocalBlockWriteAheadLogConfig config = LocalBlockWriteAheadLogConfig.builder()
                                                                        .walLogDir(WAL_DATA_DIR)
                                                                        .build();
    return new LocalBlockWriteAheadLog(config);
  }
  
  @Test
  public void testBlockStorageModuleFactory() throws Exception {
    super.testBlockStorageModuleFactory();
  }

}
