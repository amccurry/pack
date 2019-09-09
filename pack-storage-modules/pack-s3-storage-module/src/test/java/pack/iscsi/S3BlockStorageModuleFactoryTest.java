package pack.iscsi;

import java.io.File;

import org.junit.Before;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.external.local.LocalBlockWriteAheadLog;
import pack.iscsi.external.s3.S3ExternalBlockStoreFactory;
import pack.iscsi.partitioned.storagemanager.BlockIOFactory;
import pack.iscsi.partitioned.storagemanager.BlockStorageModuleFactoryTest;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
import pack.util.IOUtils;

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
    _bucket = TestProperties.getBucket();
    _objectPrefix = TestProperties.getObjectPrefix();
    S3TestSetup.cleanS3(_bucket, _objectPrefix);
  }

  @Override
  protected BlockIOFactory getBlockIOFactory() {
    return new S3ExternalBlockStoreFactory(_consistentAmazonS3, _bucket, _objectPrefix);
  }

  @Override
  protected BlockWriteAheadLog getBlockWriteAheadLog() throws Exception {
    return new LocalBlockWriteAheadLog(WAL_DATA_DIR);
  }

}
