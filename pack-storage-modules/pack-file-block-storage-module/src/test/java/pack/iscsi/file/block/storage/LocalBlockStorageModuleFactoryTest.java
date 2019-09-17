package pack.iscsi.file.block.storage;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import pack.iscsi.file.block.storage.LocalExternalBlockStoreFactory;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.volume.BlockStorageModuleFactoryTest;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog.LocalBlockWriteAheadLogConfig;

public class LocalBlockStorageModuleFactoryTest extends BlockStorageModuleFactoryTest {

  public static final File WAL_DATA_DIR = new File("./target/tmp/LocalBlockStorageModuleFactoryTest/wal");
  public static final File EXTERNAL_BLOCK_DATA_DIR = new File(
      "./target/tmp/LocalBlockStorageModuleFactoryTest/external");

  @Before
  public void setup() throws Exception {
    super.setup();
    IOUtils.rmr(EXTERNAL_BLOCK_DATA_DIR);
    IOUtils.rmr(WAL_DATA_DIR);
  }

  @Override
  protected BlockIOFactory getBlockIOFactory() {
    return new LocalExternalBlockStoreFactory(EXTERNAL_BLOCK_DATA_DIR);
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
