package pack.iscsi.partitioned.storagemanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Test;

import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.external.local.LocalExternalBlockStoreFactory;
import pack.iscsi.spi.StorageModule;
import pack.util.IOUtils;

public class BlockStorageModuleFactoryTest {

  private static final File BLOCK_DATA_DIR = new File("./target/tmp/BlockStorageModuleFactoryTest/blocks");
  private static final File EXTERNAL_BLOCK_DATA_DIR = new File("./target/tmp/BlockStorageModuleFactoryTest/external");

  @Before
  public void setup() {
    IOUtils.rmr(BLOCK_DATA_DIR);
    IOUtils.rmr(EXTERNAL_BLOCK_DATA_DIR);
  }

  @Test
  public void testBlockStorageModuleFactory() throws IOException {
    BlockStore blockStore = getBlockStore();
    ExternalBlockIOFactory externalBlockStoreFactory = new LocalExternalBlockStoreFactory(EXTERNAL_BLOCK_DATA_DIR);
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();

    long maxCacheSizeInBytes = 50_000_000;
    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .blockDataDir(BLOCK_DATA_DIR)
                                                                            .blockStore(blockStore)
                                                                            .externalBlockStoreFactory(
                                                                                externalBlockStoreFactory)
                                                                            .writeAheadLog(writeAheadLog)
                                                                            .maxCacheSizeInBytes(maxCacheSizeInBytes)
                                                                            .build();

    BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config);
    StorageModule storageModule = factory.getStorageModule("test");
    assertEquals(195311, storageModule.getSizeInBlocks());

    long seed = 1;// new Random().nextLong();

    long length = 51_000_000;
    {
      byte[] buffer1 = new byte[9876];
      byte[] buffer2 = new byte[9876];
      Random random = new Random(seed);
      for (long pos = 0; pos < length; pos += buffer1.length) {
        random.nextBytes(buffer1);
        storageModule.write(buffer1, pos);
        storageModule.read(buffer2, pos);
        assertTrue(Arrays.equals(buffer1, buffer2));
      }
    }
    {
      byte[] buffer1 = new byte[9876];
      byte[] buffer2 = new byte[9876];
      Random random = new Random(seed);
      for (long pos = 0; pos < length; pos += buffer1.length) {
        random.nextBytes(buffer1);
        storageModule.read(buffer2, pos);
        assertTrue("pos " + pos, Arrays.equals(buffer1, buffer2));
      }
    }

  }

  private BlockStore getBlockStore() {
    ConcurrentHashMap<Long, Long> gens = new ConcurrentHashMap<>();
    return new BlockStore() {

      @Override
      public void updateGeneration(long volumeId, long blockId, long generation) throws IOException {
        gens.put(blockId, generation);
      }

      @Override
      public long getGeneration(long volumeId, long blockId) throws IOException {
        Long gen = gens.get(blockId);
        if (gen == null) {
          return 0;
        }
        return gen;
      }

      @Override
      public List<String> getVolumeNames() {
        return Arrays.asList("test");
      }

      @Override
      public long getVolumeId(String name) {
        return 12345;
      }

      @Override
      public long getLengthInBytes(long volumeId) {
        return 100_000_000L;
      }

      @Override
      public int getBlockSize(long volumeId) {
        return 100_000;
      }
    };
  }

  private BlockWriteAheadLog getBlockWriteAheadLog() {
    return new BlockWriteAheadLog() {
      @Override
      public void write(long volumeId, long blockId, long generation, byte[] bytes, int offset, int len)
          throws IOException {

      }
    };
  }

}
