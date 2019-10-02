package pack.iscsi.volume;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.block.BlockCacheMetadataStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

public abstract class BlockStorageModuleFactoryTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModuleFactoryTest.class);

  @Before
  public void setup() throws Exception {
    clearBlockData();
    clearWalData();
    clearStateData();
  }

  protected abstract void clearBlockData();

  protected abstract void clearWalData();

  protected abstract void clearStateData();

  protected abstract File getBlockDataDir();

  protected abstract BlockIOFactory getBlockIOFactory() throws Exception;

  protected abstract BlockWriteAheadLog getBlockWriteAheadLog() throws Exception;

  protected abstract BlockGenerationStore getBlockGenerationStore() throws Exception;

  protected abstract BlockStateStore getBlockStateStore() throws Exception;

  protected abstract BlockCacheMetadataStore getBlockCacheMetadataStore() throws Exception;

  @Test
  public void testBlockStorageModuleFactory() throws Exception {
    PackVolumeStore volumeStore = getPackVolumeStore(12345, 100_000, 100_000_000L);
    BlockGenerationStore blockStore = getBlockGenerationStore();
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory();
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();
    BlockStateStore blockStateStore = getBlockStateStore();
    BlockCacheMetadataStore blockCacheMetadataStore = getBlockCacheMetadataStore();

    long maxCacheSizeInBytes = 50_000_000;
    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .blockCacheMetadataStore(
                                                                                blockCacheMetadataStore)
                                                                            .packVolumeStore(volumeStore)
                                                                            .blockDataDir(getBlockDataDir())
                                                                            .blockStateStore(blockStateStore)
                                                                            .blockStore(blockStore)
                                                                            .externalBlockStoreFactory(
                                                                                externalBlockStoreFactory)
                                                                            .writeAheadLog(writeAheadLog)
                                                                            .maxCacheSizeInBytes(maxCacheSizeInBytes)
                                                                            .build();

    String volumeName = "test";

    try (BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config)) {
      volumeStore.assignVolume(volumeName);
      StorageModule storageModule = factory.getStorageModule(volumeName);
      assertEquals(195311, storageModule.getSizeInBlocks());
      long seed = new Random().nextLong();
      long length = 51_000_000;
      readsAndWritesTest(storageModule, seed, length);
      readsOnlyTest(storageModule, seed, length);
      volumeStore.unassignVolume(volumeName);
    }
  }

  @Test
  public void testBlockStorageModuleFactoryWithClosingAndReOpen() throws Exception {
    PackVolumeStore volumeStore = getPackVolumeStore(12345, 100_000, 100_000_000L);
    BlockGenerationStore blockStore = getBlockGenerationStore();
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory();
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();
    BlockStateStore blockStateStore = getBlockStateStore();
    BlockCacheMetadataStore blockCacheMetadataStore = getBlockCacheMetadataStore();

    long maxCacheSizeInBytes = 50_000_000;
    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .blockCacheMetadataStore(
                                                                                blockCacheMetadataStore)
                                                                            .packVolumeStore(volumeStore)
                                                                            .blockDataDir(getBlockDataDir())
                                                                            .blockStateStore(blockStateStore)
                                                                            .blockStore(blockStore)
                                                                            .externalBlockStoreFactory(
                                                                                externalBlockStoreFactory)
                                                                            .writeAheadLog(writeAheadLog)
                                                                            .maxCacheSizeInBytes(maxCacheSizeInBytes)
                                                                            .build();

    long seed = new Random().nextLong();
    long length = 51_000_000;

    String volumeName = "test";

    try (BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config)) {
      volumeStore.assignVolume(volumeName);
      try (StorageModule storageModule = factory.getStorageModule("test")) {
        assertEquals(195311, storageModule.getSizeInBlocks());
        readsAndWritesTest(storageModule, seed, length);
      }
      try (StorageModule storageModule = factory.getStorageModule("test")) {
        assertEquals(195311, storageModule.getSizeInBlocks());
        readsOnlyTest(storageModule, seed, length);
      }
      volumeStore.unassignVolume(volumeName);
    }
  }

  @Test
  public void testBlockStorageModuleFactoryWithClosingAndReOpenWithClearedBlocks() throws Exception {
    PackVolumeStore volumeStore = getPackVolumeStore(12345, 100_000, 100_000_000L);
    BlockGenerationStore blockStore = getBlockGenerationStore();
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory();
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();
    BlockStateStore blockStateStore = getBlockStateStore();
    BlockCacheMetadataStore blockCacheMetadataStore = getBlockCacheMetadataStore();

    long maxCacheSizeInBytes = 50_000_000;

    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .blockCacheMetadataStore(
                                                                                blockCacheMetadataStore)
                                                                            .packVolumeStore(volumeStore)
                                                                            .blockDataDir(getBlockDataDir())
                                                                            .blockStateStore(blockStateStore)
                                                                            .blockStore(blockStore)
                                                                            .externalBlockStoreFactory(
                                                                                externalBlockStoreFactory)
                                                                            .writeAheadLog(writeAheadLog)
                                                                            .maxCacheSizeInBytes(maxCacheSizeInBytes)
                                                                            .build();

    long seed = new Random().nextLong();
    long length = 51_000_000;

    String volumeName = "test";

    try (BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config)) {
      volumeStore.assignVolume(volumeName);
      try (StorageModule storageModule = factory.getStorageModule("test")) {
        assertEquals(195311, storageModule.getSizeInBlocks());
        readsAndWritesTest(storageModule, seed, length);
      }
    }
    clearBlockData();
    clearStateData();
    try (BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config)) {
      try (StorageModule storageModule = factory.getStorageModule("test")) {
        assertEquals(195311, storageModule.getSizeInBlocks());
        readsOnlyTest(storageModule, seed, length);
      }
      volumeStore.unassignVolume(volumeName);
    }
  }

  @Test
  public void testBlockStorageModuleFactoryRecoverBlockThatOnlyExistsInWal() throws Exception {
    PackVolumeStore volumeStore = getPackVolumeStore(12345, 100_000, 100_000_000L);
    BlockGenerationStore blockStore = getBlockGenerationStore();
    BlockIOFactory externalBlockStoreFactory = getBlockIOFactory();
    BlockWriteAheadLog writeAheadLog = getBlockWriteAheadLog();
    BlockStateStore blockStateStore = getBlockStateStore();
    BlockCacheMetadataStore blockCacheMetadataStore = getBlockCacheMetadataStore();

    long maxCacheSizeInBytes = 50_000_000;

    BlockStorageModuleFactoryConfig config = BlockStorageModuleFactoryConfig.builder()
                                                                            .blockCacheMetadataStore(
                                                                                blockCacheMetadataStore)
                                                                            .packVolumeStore(volumeStore)
                                                                            .blockDataDir(getBlockDataDir())
                                                                            .blockStateStore(blockStateStore)
                                                                            .blockStore(blockStore)
                                                                            .externalBlockStoreFactory(
                                                                                externalBlockStoreFactory)
                                                                            .writeAheadLog(writeAheadLog)
                                                                            .maxCacheSizeInBytes(maxCacheSizeInBytes)
                                                                            .build();

    long seed = new Random().nextLong();
    String volumeName = "test";
    List<Closeable> closeList = new ArrayList<>();

    {
      BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config);
      closeList.add(factory);
      volumeStore.assignVolume(volumeName);
      StorageModule storageModule = factory.getStorageModule("test");
      closeList.add(storageModule);
      assertEquals(195311, storageModule.getSizeInBlocks());
      readsAndWritesTest(storageModule, seed, 9876);
    }
    clearBlockData();
    clearStateData();
    try (BlockStorageModuleFactory factory = new BlockStorageModuleFactory(config)) {
      try (StorageModule storageModule = factory.getStorageModule("test")) {
        assertEquals(195311, storageModule.getSizeInBlocks());
        readsOnlyTest(storageModule, seed, 9876);
      }
      volumeStore.unassignVolume(volumeName);
    }

    IOUtils.close(LOGGER, closeList);
  }

  private void readsAndWritesTest(StorageModule storageModule, long seed, long length) throws IOException {
    byte[] buffer1 = new byte[9876];
    byte[] buffer2 = new byte[9876];
    Random random = new Random(seed);
    for (long pos = 0; pos < length; pos += buffer1.length) {

      random.nextBytes(buffer1);
      storageModule.write(buffer1, pos);
      storageModule.flushWrites();
      storageModule.read(buffer2, pos);

      for (int i = 0; i < buffer1.length; i++) {
        assertEquals("seed=" + seed + " pos=" + pos + " i=" + i, buffer1[i], buffer2[i]);
      }

      assertTrue("seed=" + seed + " pos=" + pos, Arrays.equals(buffer1, buffer2));
    }
  }

  private void readsOnlyTest(StorageModule storageModule, long seed, long length) throws IOException {
    byte[] buffer1 = new byte[9876];
    byte[] buffer2 = new byte[9876];
    Random random = new Random(seed);
    for (long pos = 0; pos < length; pos += buffer1.length) {
      random.nextBytes(buffer1);
      storageModule.read(buffer2, pos);
      for (int i = 0; i < buffer1.length; i++) {
        assertEquals("pos " + pos + " " + i, buffer1[i], buffer2[i]);
      }
    }
  }

  private PackVolumeStore getPackVolumeStore(long volumeId, int blockSize, long lengthInBytes) {
    Set<String> assigned = new HashSet<>();
    return new PackVolumeStore() {

      @Override
      public PackVolumeMetadata getVolumeMetadata(long volumeId) throws IOException {
        return PackVolumeMetadata.builder()
                                 .blockSizeInBytes(blockSize)
                                 .lengthInBytes(lengthInBytes)
                                 .volumeId(volumeId)
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
        return new ArrayList<String>(assigned);
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
        assigned.add(name);
      }

      @Override
      public void unassignVolume(String name) throws IOException {
        assigned.remove(name);
      }

      @Override
      public void renameVolume(String name, String newName) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void createSnapshot(String name, String snapshotName) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public List<String> listSnapshots(String name) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void deleteSnapshot(String name, String snapshotName) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void sync(String name) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void cloneVolume(String name, String existingVolume, String snapshotId) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(String name, String snapshotId) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(long volumeId, String snapshotId) throws IOException {
        throw new RuntimeException("not impl");
      }

    };
  }
}
