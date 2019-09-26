package pack.iscsi.file.block.storage;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Test;

import pack.iscsi.block.LocalBlockStateStore;
import pack.iscsi.block.LocalBlockStateStoreConfig;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.volume.BlockStorageModuleFactoryTest;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog.LocalBlockWriteAheadLogConfig;

public class LocalBlockStorageModuleFactoryTest extends BlockStorageModuleFactoryTest {

  public static final File WAL_DATA_DIR = new File("./target/tmp/LocalBlockStorageModuleFactoryTest/wal");
  public static final File EXTERNAL_BLOCK_DATA_DIR = new File(
      "./target/tmp/LocalBlockStorageModuleFactoryTest/external");
  public static final File BLOCK_STATE_DIR = new File("./target/tmp/LocalBlockStorageModuleFactoryTest/state");

  @Before
  public void setup() throws Exception {
    super.setup();
    IOUtils.rmr(EXTERNAL_BLOCK_DATA_DIR);
    IOUtils.rmr(WAL_DATA_DIR);
    IOUtils.rmr(BLOCK_STATE_DIR);
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

  @Override
  protected BlockStateStore getBlockStateStore() {
    LocalBlockStateStoreConfig config = LocalBlockStateStoreConfig.builder()
                                                                  .blockStateDir(BLOCK_STATE_DIR)
                                                                  .build();
    return new LocalBlockStateStore(config);
  }

  @Override
  protected BlockGenerationStore getBlockGenerationStore() throws Exception {
    Map<Long, Long> gens = new ConcurrentHashMap<>();
    return new BlockGenerationStore() {

      @Override
      public void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException {
        gens.put(blockId, lastStoredGeneration);
      }

      @Override
      public long getLastStoreGeneration(long volumeId, long blockId) throws IOException {
        Long gen = gens.get(blockId);
        if (gen == null) {
          return Block.MISSING_BLOCK_GENERATION;
        }
        return gen;
      }
    };
  }

  @Test
  public void testBlockStorageModuleFactoryWithClosingAndReOpen() throws Exception {
    super.testBlockStorageModuleFactoryWithClosingAndReOpen();
  }

  @Test
  public void testBlockStorageModuleFactoryWithClosingAndReOpenWithClearedBlocks() throws Exception {
    super.testBlockStorageModuleFactoryWithClosingAndReOpenWithClearedBlocks();
  }

  @Test
  public void testBlockStorageModuleFactory() throws Exception {
    super.testBlockStorageModuleFactory();
  }
}
