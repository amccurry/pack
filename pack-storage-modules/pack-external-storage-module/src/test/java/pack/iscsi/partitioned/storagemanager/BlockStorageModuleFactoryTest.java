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

import pack.iscsi.partitioned.block.Block;
import pack.iscsi.spi.StorageModule;
import pack.util.IOUtils;

public abstract class BlockStorageModuleFactoryTest {

  public static final File BLOCK_DATA_DIR = new File("./target/tmp/BlockStorageModuleFactoryTest/blocks");

  @Before
  public void setup() throws Exception {
    IOUtils.rmr(BLOCK_DATA_DIR);
  }

  protected abstract BlockIOFactory getBlockIOFactory() throws Exception;

  protected abstract BlockWriteAheadLog getBlockWriteAheadLog() throws Exception;

  @Test
  public void testBlockStorageModuleFactory() throws Exception {
    VolumeStore volumeStore = getVolumeStore(12345, 100_000, 100_000_000L);
    BlockGenerationStore blockStore = getBlockStore();
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory();
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();

    long maxCacheSizeInBytes = 50_000_000;
    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .volumeStore(volumeStore)
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
  public void testBlockStorageModuleFactoryWithClosingAndReOpen() throws Exception {
    VolumeStore volumeStore = getVolumeStore(12345, 100_000, 100_000_000L);
    BlockGenerationStore blockStore = getBlockStore();
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory();
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();

    long maxCacheSizeInBytes = 50_000_000;
    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .volumeStore(volumeStore)
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
  public void testBlockStorageModuleFactoryWithClosingAndReOpenWithClearedBlocks() throws Exception {
    VolumeStore volumeStore = getVolumeStore(12345, 100_000, 100_000_000L);
    BlockGenerationStore blockStore = getBlockStore();
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory();
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();

    long maxCacheSizeInBytes = 50_000_000;
    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .volumeStore(volumeStore)
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

  private BlockGenerationStore getBlockStore() {

    ConcurrentHashMap<Long, Long> gens = new ConcurrentHashMap<>();

    return new BlockGenerationStore() {

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

    };
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

  private VolumeStore getVolumeStore(long volumeId, int blockSize, long lengthInBytes) {
    return new VolumeStore() {

      @Override
      public List<String> getVolumeNames() {
        throw new RuntimeException("Not impl");
      }

      @Override
      public VolumeMetadata getVolumeMetadata(long volumeId) throws IOException {
        return VolumeMetadata.builder()
                             .blockSize(blockSize)
                             .lengthInBytes(lengthInBytes)
                             .build();
      }

      @Override
      public void createVolume(String name, int blockSize, long lengthInBytes) throws IOException {
        throw new RuntimeException("Not impl");
      }

      @Override
      public VolumeMetadata getVolumeMetadata(String name) throws IOException {
        return VolumeMetadata.builder()
                             .blockSize(blockSize)
                             .lengthInBytes(lengthInBytes)
                             .volumeId(volumeId)
                             .build();
      }

      @Override
      public void destroyVolume(String name) throws IOException {
        throw new RuntimeException("Not impl");
      }

      @Override
      public void renameVolume(String existingName, String newName) throws IOException {
        throw new RuntimeException("Not impl");
      }

      @Override
      public void growVolume(String name, long lengthInBytes) throws IOException {
        throw new RuntimeException("Not impl");
      }
    };
  }
}
