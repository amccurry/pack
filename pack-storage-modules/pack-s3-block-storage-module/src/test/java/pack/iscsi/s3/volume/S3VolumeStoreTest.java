package pack.iscsi.s3.volume;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Splitter;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;
import pack.iscsi.s3.S3TestProperties;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.util.S3Utils;
import pack.iscsi.s3.util.S3Utils.ListResultProcessor;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.VolumeListener;

public class S3VolumeStoreTest {

  private static final String BLOCK = "/block/";

  private static final Logger LOGGER = LoggerFactory.getLogger(S3VolumeStoreTest.class);

  private static ConsistentAmazonS3 CONSISTENT_AMAZON_S3;
  private static String BUCKET;
  private static String OBJECT_PREFIX;

  @BeforeClass
  public static void setup() throws Exception {
    CONSISTENT_AMAZON_S3 = S3TestSetup.getConsistentAmazonS3();
    BUCKET = S3TestProperties.getBucket();
    OBJECT_PREFIX = S3TestProperties.getObjectPrefix();
    S3TestSetup.cleanS3(BUCKET, OBJECT_PREFIX);
  }

  @Test
  public void testS3VolumeStoreCreate() throws Exception {
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(BUCKET)
                                                    .objectPrefix(OBJECT_PREFIX)
                                                    .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                    .build();

    try (S3VolumeStore store = new S3VolumeStore(config)) {
      store.createVolume("test", 100_000_000, 10_000);
      PackVolumeMetadata volumeMetadata = store.getVolumeMetadata("test");
      assertEquals(10_000, volumeMetadata.getBlockSizeInBytes());
      assertEquals(100_000_000, volumeMetadata.getLengthInBytes());
    }
  }

  @Test
  public void testS3VolumeStoreList() throws Exception {
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(BUCKET)
                                                    .objectPrefix(OBJECT_PREFIX)
                                                    .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                    .build();

    try (S3VolumeStore store = new S3VolumeStore(config)) {
      store.createVolume("test1", 10_000, 100_000_000);
      store.createVolume("test2", 20_000, 200_000_000);

      List<String> list = new ArrayList<>(store.getAllVolumes());
      Collections.sort(list);
      assertEquals(Arrays.asList("test1", "test2"), list);
    }
  }

  @Test
  public void testS3VolumeStoreAttachedList() throws Exception {
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(BUCKET)
                                                    .objectPrefix(OBJECT_PREFIX)
                                                    .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                    .build();

    try (S3VolumeStore store = new S3VolumeStore(config)) {

      assertTrue(store.getAttachedVolumes()
                      .isEmpty());

      store.createVolume("testAttached", 10_000, 100_000_000);

      assertTrue(store.getAllVolumes()
                      .contains("testAttached"));

      store.attachVolume("testAttached");

      assertEquals(Arrays.asList("testAttached"), store.getAttachedVolumes());

      store.detachVolume("testAttached");

      assertTrue(store.getAttachedVolumes()
                      .isEmpty());

      store.deleteVolume("testAttached");

      assertTrue(store.getAttachedVolumes()
                      .isEmpty());
    }
  }

  @Test
  public void testS3VolumeStoreRename() throws Exception {
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(BUCKET)
                                                    .objectPrefix(OBJECT_PREFIX)
                                                    .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                    .build();

    try (S3VolumeStore store = new S3VolumeStore(config)) {
      store.createVolume("test3", 100_000_000, 10_000);
      long volumeId;
      {
        PackVolumeMetadata volumeMetadata = store.getVolumeMetadata("test3");
        assertEquals(10_000, volumeMetadata.getBlockSizeInBytes());
        assertEquals(100_000_000, volumeMetadata.getLengthInBytes());
        volumeId = volumeMetadata.getVolumeId();
      }
      store.renameVolume("test3", "test4");
      {
        PackVolumeMetadata volumeMetadata = store.getVolumeMetadata("test4");
        assertEquals(10_000, volumeMetadata.getBlockSizeInBytes());
        assertEquals(100_000_000, volumeMetadata.getLengthInBytes());
        assertEquals(volumeId, volumeMetadata.getVolumeId());
      }

    }
  }

  @Test
  public void testS3VolumeStoreSnapshot() throws Exception {
    S3VolumeStoreConfig config = S3VolumeStoreConfig.builder()
                                                    .bucket(BUCKET)
                                                    .objectPrefix(OBJECT_PREFIX)
                                                    .consistentAmazonS3(CONSISTENT_AMAZON_S3)
                                                    .build();

    String name = "test5";
    try (S3VolumeStore store = new S3VolumeStore(config)) {
      VolumeListener volumeListener = getVolumeListener(name);
      store.register(volumeListener);
      store.createVolume(name, 100_000_000, 10_000);
      store.attachVolume(name);

      PackVolumeMetadata metadata = store.getVolumeMetadata(name);
      long volumeId = metadata.getVolumeId();

      String cachedBlockInfoKey = S3Utils.getCachedBlockInfo(OBJECT_PREFIX, volumeId);
      CONSISTENT_AMAZON_S3.putObject(BUCKET, cachedBlockInfoKey, "");

      int genCount = 10;

      createMockBlocksAndGenerations(metadata, genCount, 1);
      store.createSnapshot(name, "s1");
      assertEquals(Arrays.asList("s1"), store.listSnapshots(name));
      store.gc(name);

      assertBlocksAndGenerations(volumeId, bg(0, 10), bg(1, 10));
      // assert blocks and gens left after gc

      createMockBlocksAndGenerations(metadata, genCount, 95);
      store.createSnapshot(name, "s2");
      assertEquals(Arrays.asList("s1", "s2"), store.listSnapshots(name));
      store.gc(name);
      assertBlocksAndGenerations(volumeId, bg(0, 10), bg(1, 10), bg(0, 104), bg(1, 104));
      // assert blocks and gens left after gc

      store.deleteSnapshot(name, "s1");
      assertEquals(Arrays.asList("s2"), store.listSnapshots(name));
      store.gc(name);
      assertBlocksAndGenerations(volumeId, bg(0, 104), bg(1, 104));
      // assert blocks and gens left after gc

      store.deleteSnapshot(name, "s2");
      assertTrue(store.listSnapshots(name)
                      .isEmpty());
      store.gc(name);
      assertBlocksAndGenerations(volumeId, bg(0, 104), bg(1, 104));
      // assert blocks and gens left after gc
    }
  }

  private VolumeListener getVolumeListener(String name) {
    return new VolumeListener() {

      @Override
      public Map<BlockKey, Long> createSnapshot(PackVolumeMetadata packVolumeMetadata) throws IOException {
        Map<BlockKey, Long> result = new HashMap<>();
        String prefix = S3Utils.getVolumeBlocksPrefix(OBJECT_PREFIX, packVolumeMetadata.getVolumeId());
        S3Utils.listObjects(CONSISTENT_AMAZON_S3.getClient(), BUCKET, prefix, summary -> {
          String key = summary.getKey();
          int indexOf = key.indexOf(BLOCK);
          String string = key.substring(indexOf + BLOCK.length());
          List<String> list = Splitter.on('/')
                                      .splitToList(string);
          BlockKey blockKey = BlockKey.builder()
                                      .volumeId(packVolumeMetadata.getVolumeId())
                                      .blockId(Long.parseLong(list.get(0)))
                                      .build();
          long generation = Long.parseLong(list.get(1));
          Long currentMax = result.get(blockKey);
          if (currentMax == null || generation > currentMax) {
            result.put(blockKey, generation);
          }
        });
        return result;
      }

      @Override
      public boolean hasVolume(PackVolumeMetadata metadata) throws IOException {
        return true;
      }

    };
  }

  private void assertBlocksAndGenerations(long volumeId, BlockGen... blockGens) {
    String prefix = S3Utils.getVolumeBlocksPrefix(OBJECT_PREFIX, volumeId);
    Set<BlockGen> actual = new HashSet<>();
    S3Utils.listObjects(CONSISTENT_AMAZON_S3.getClient(), BUCKET, prefix, new ListResultProcessor() {
      @Override
      public void addResult(S3ObjectSummary summary) {
        String key = summary.getKey();
        int indexOf = key.indexOf(BLOCK);
        String string = key.substring(indexOf + BLOCK.length());
        List<String> list = Splitter.on('/')
                                    .splitToList(string);
        actual.add(BlockGen.builder()
                           .blockId(Long.parseLong(list.get(0)))
                           .generation(Long.parseLong(list.get(1)))
                           .build());
      }
    });

    assertEquals(new HashSet<>(Arrays.asList(blockGens)), actual);
  }

  private BlockGen bg(long blockId, long generation) {
    return BlockGen.builder()
                   .blockId(blockId)
                   .generation(generation)
                   .build();
  }

  @Value
  @Builder
  public static class BlockGen {
    long blockId;
    long generation;
  }

  private void createMockBlocksAndGenerations(PackVolumeMetadata metadata, long genCount, long startingGeneration) {
    // create mock blocks and gens
    for (long blockId = 0; blockId < 2; blockId++) {
      for (long g = 0; g < genCount; g++) {
        long generation = g + startingGeneration;
        LOGGER.info("create mock block {} generation {}", blockId, generation);
        createMockBlock(metadata, blockId, generation);
      }
    }
  }

  private void createMockBlock(PackVolumeMetadata metadata, long blockId, long generation) {
    String key = S3Utils.getBlockGenerationKey(OBJECT_PREFIX, metadata.getVolumeId(), blockId, generation);
    CONSISTENT_AMAZON_S3.putObject(BUCKET, key, metadata.getName());
  }
}
