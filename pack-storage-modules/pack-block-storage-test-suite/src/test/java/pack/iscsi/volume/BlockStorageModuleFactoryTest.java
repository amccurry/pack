package pack.iscsi.volume;

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

import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

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
    PackVolumeStore volumeStore = getPackVolumeStore(12345, 100_000, 100_000_000L);
    BlockGenerationStore blockStore = getBlockStore();
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory();
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();

    long maxCacheSizeInBytes = 50_000_000;
    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .packVolumeStore(volumeStore)
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
    PackVolumeStore volumeStore = getPackVolumeStore(12345, 100_000, 100_000_000L);
    BlockGenerationStore blockStore = getBlockStore();
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory();
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();

    long maxCacheSizeInBytes = 50_000_000;
    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .packVolumeStore(volumeStore)
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
    PackVolumeStore volumeStore = getPackVolumeStore(12345, 100_000, 100_000_000L);
    BlockGenerationStore blockStore = getBlockStore();
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory();
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();

    long maxCacheSizeInBytes = 50_000_000;
    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .packVolumeStore(volumeStore)
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

      if (pos == 98760) {
        System.out.println();
      }

      storageModule.write(buffer1, pos);
      storageModule.read(buffer2, pos);

      for (int i = 0; i < buffer1.length; i++) {
        assertEquals("pos=" + pos + " i=" + i, buffer1[i], buffer2[i]);
      }

      assertTrue("pos=" + pos, Arrays.equals(buffer1, buffer2));
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

  private PackVolumeStore getPackVolumeStore(long volumeId, int blockSize, long lengthInBytes) {
    return new PackVolumeStore() {

      @Override
      public PackVolumeMetadata getVolumeMetadata(long volumeId) throws IOException {
        return PackVolumeMetadata.builder()
                                 .blockSizeInBytes(blockSize)
                                 .lengthInBytes(lengthInBytes)
                                 .build();
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(String name) throws IOException {
        return PackVolumeMetadata.builder()
                                 .blockSizeInBytes(blockSize)
                                 .lengthInBytes(lengthInBytes)
                                 .volumeId(volumeId)
                                 .build();
      }

      @Override
      public List<String> getAllVolumes() throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public List<String> getAssignedVolumes() throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void createVolume(String name, long lengthInBytes, int blockSizeInBytes) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void deleteVolume(String name) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void growVolume(String name, long newLengthInBytes) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void assignVolume(String name) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void unassignVolume(String name) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void renameVolume(String name, String newName) throws IOException {
        throw new RuntimeException("not impl");
      }

    };
  }
}
