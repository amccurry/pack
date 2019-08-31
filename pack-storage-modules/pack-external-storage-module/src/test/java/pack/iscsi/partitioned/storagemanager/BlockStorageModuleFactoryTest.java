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

import pack.iscsi.external.local.LocalBlockWriteAheadLog;
import pack.iscsi.external.local.LocalExternalBlockStoreFactory;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.spi.StorageModule;
import pack.util.IOUtils;

public class BlockStorageModuleFactoryTest {

  private static final File BLOCK_DATA_DIR = new File("./target/tmp/BlockStorageModuleFactoryTest/blocks");
  private static final File WAL_DATA_DIR = new File("./target/tmp/BlockStorageModuleFactoryTest/wal");
  private static final File EXTERNAL_BLOCK_DATA_DIR = new File("./target/tmp/BlockStorageModuleFactoryTest/external");

  @Before
  public void setup() {
    IOUtils.rmr(BLOCK_DATA_DIR);
    IOUtils.rmr(EXTERNAL_BLOCK_DATA_DIR);
    IOUtils.rmr(WAL_DATA_DIR);
  }

  @Test
  public void testBlockStorageModuleFactory() throws IOException {
    BlockStore blockStore = getBlockStore();
    BlockIOFactory externalBlockStoreFactory = new LocalExternalBlockStoreFactory(EXTERNAL_BLOCK_DATA_DIR);
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

    try (BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config)) {
      StorageModule storageModule = factory.getStorageModule("test");
      assertEquals(195311, storageModule.getSizeInBlocks());
      long seed = new Random().nextLong();
      long length = 51_000_000;
      readsAndWritesTest(storageModule, seed, length);
      readsOnlyTest(storageModule, seed, length);
    }
  }

  @Test
  public void testBlockStorageModuleFactoryWithClosingAndReOpen() throws IOException {
    BlockStore blockStore = getBlockStore();
    BlockIOFactory externalBlockStoreFactory = new LocalExternalBlockStoreFactory(EXTERNAL_BLOCK_DATA_DIR);
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

    long seed = new Random().nextLong();
    long length = 51_000_000;

    try (BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config)) {
      try (StorageModule storageModule = factory.getStorageModule("test")) {
        assertEquals(195311, storageModule.getSizeInBlocks());
        readsAndWritesTest(storageModule, seed, length);
      }

      try (StorageModule storageModule = factory.getStorageModule("test")) {
        assertEquals(195311, storageModule.getSizeInBlocks());
        readsOnlyTest(storageModule, seed, length);
      }
    }
  }

  @Test
  public void testBlockStorageModuleFactoryWithClosingAndReOpenWithClearedBlocks() throws IOException {
    BlockStore blockStore = getBlockStore();
    BlockIOFactory externalBlockStoreFactory = new LocalExternalBlockStoreFactory(EXTERNAL_BLOCK_DATA_DIR);
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

    long seed = new Random().nextLong();
    long length = 51_000_000;

    try (BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config)) {
      try (StorageModule storageModule = factory.getStorageModule("test")) {
        assertEquals(195311, storageModule.getSizeInBlocks());
        readsAndWritesTest(storageModule, seed, length);
      }
    }
    IOUtils.rmr(BLOCK_DATA_DIR);
    try (BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config)) {
      try (StorageModule storageModule = factory.getStorageModule("test")) {
        assertEquals(195311, storageModule.getSizeInBlocks());
        readsOnlyTest(storageModule, seed, length);
      }
    }
  }

  private BlockStore getBlockStore() {

    ConcurrentHashMap<Long, Long> gens = new ConcurrentHashMap<>();

    return new BlockStore() {

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

      @Override
      public long getLastStoreGeneration(long volumeId, long blockId) {
        Long gen = gens.get(blockId);
        if (gen == null) {
          return Block.MISSING_BLOCK_GENERATION;
        }
        return gen;
      }

      @Override
      public void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) {
        gens.put(blockId, lastStoredGeneration);
      }

      @Override
      public long createVolume(String name, int blockSize, long lengthInBytes) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void destroyVolume(long volumeId) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void renameVolume(long volumeId, String name) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void growVolume(long volumeId, long lengthInBytes) throws IOException {
        throw new RuntimeException("not impl");
      }
    };
  }

  private BlockWriteAheadLog getBlockWriteAheadLog() throws IOException {
    return new LocalBlockWriteAheadLog(WAL_DATA_DIR);
  }

  private void readsAndWritesTest(StorageModule storageModule, long seed, long length) throws IOException {
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

  private void readsOnlyTest(StorageModule storageModule, long seed, long length) throws IOException {
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
