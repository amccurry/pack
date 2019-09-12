package pack.iscsi.s3.volume;

import java.io.File;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.bk.BKTestSetup;
import pack.iscsi.bk.wal.BookKeeperWriteAheadLog;
import pack.iscsi.bk.wal.BookKeeperWriteAheadLogConfig;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.TestProperties;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory;
import pack.iscsi.s3.block.S3ExternalBlockStoreFactory.S3ExternalBlockStoreFactoryConfig;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.volume.BlockIOFactory;
import pack.iscsi.volume.BlockStorageModuleFactoryTest;
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
    S3ExternalBlockStoreFactoryConfig config = S3ExternalBlockStoreFactoryConfig.builder()
                                                                                .bucket(_bucket)
                                                                                .consistentAmazonS3(_consistentAmazonS3)
                                                                                .objectPrefix(_objectPrefix)
                                                                                .build();
    return new S3ExternalBlockStoreFactory(config);
  }

  @Override
  protected BlockWriteAheadLog getBlockWriteAheadLog() throws Exception {
    BookKeeper bookKeeper = BKTestSetup.getBookKeeper();
    CuratorFramework curatorFramework = BKTestSetup.getCuratorFramework();
    BookKeeperWriteAheadLogConfig config = BookKeeperWriteAheadLogConfig.builder()
                                                                        .ackQuorumSize(1)
                                                                        .writeQuorumSize(1)
                                                                        .ensSize(1)
                                                                        .bookKeeper(bookKeeper)
                                                                        .curatorFramework(curatorFramework)
                                                                        .build();

    return new BookKeeperWriteAheadLog(config);
  }

}
