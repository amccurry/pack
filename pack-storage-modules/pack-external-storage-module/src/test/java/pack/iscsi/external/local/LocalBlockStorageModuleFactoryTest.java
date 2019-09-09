package pack.iscsi.external.local;

import java.io.File;

import org.junit.Before;

import pack.iscsi.partitioned.storagemanager.BlockIOFactory;
import pack.iscsi.partitioned.storagemanager.BlockStorageModuleFactoryTest;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
import pack.util.IOUtils;

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
    return new LocalBlockWriteAheadLog(WAL_DATA_DIR);
  }

}
