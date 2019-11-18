package pack.iscsi.file.block.storage;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;

import pack.iscsi.block.LocalBlockStateStore;
import pack.iscsi.block.LocalBlockStateStoreConfig;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockCacheMetadataStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.volume.BlockStorageModuleFactoryTest;

public class LocalBlockStorageModuleFactoryTest extends BlockStorageModuleFactoryTest {

  public static final File WAL_DATA_DIR = new File("./target/tmp/LocalBlockStorageModuleFactoryTest/wal");
  public static final File EXTERNAL_BLOCK_DATA_DIR = new File(
      "./target/tmp/LocalBlockStorageModuleFactoryTest/external");
  public static final File[] BLOCK_DATA_DIRS = new File[] {
      new File("./target/tmp/LocalBlockStorageModuleFactoryTest/block0"),
      new File("./target/tmp/LocalBlockStorageModuleFactoryTest/block1") };
  public static final File BLOCK_STATE_DIR = new File("./target/tmp/LocalBlockStorageModuleFactoryTest/state");

  @Before
  public void setup() throws Exception {
    IOUtils.rmr(BLOCK_DATA_DIRS);
    IOUtils.rmr(EXTERNAL_BLOCK_DATA_DIR);
    IOUtils.rmr(WAL_DATA_DIR);
    IOUtils.rmr(BLOCK_STATE_DIR);
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
    return new LocalExternalBlockStoreFactory(EXTERNAL_BLOCK_DATA_DIR);
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
      public void setLastStoredGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException {
        gens.put(blockId, lastStoredGeneration);
      }

      @Override
      public long getLastStoredGeneration(long volumeId, long blockId) throws IOException {
        Long gen = gens.get(blockId);
        if (gen == null) {
          return Block.MISSING_BLOCK_GENERATION;
        }
        return gen;
      }

      @Override
      public Map<BlockKey, Long> getAllLastStoredGeneration(long volumeId) throws IOException {
        throw new RuntimeException("not impl");
      }
    };
  }

  @Override
  protected BlockCacheMetadataStore getBlockCacheMetadataStore() throws Exception {
    return new BlockCacheMetadataStore() {
    };
  }

}
