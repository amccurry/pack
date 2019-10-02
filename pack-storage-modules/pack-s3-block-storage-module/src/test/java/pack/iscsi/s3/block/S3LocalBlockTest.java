package pack.iscsi.s3.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Before;
import org.junit.Test;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.block.LocalBlock;
import pack.iscsi.block.LocalBlockConfig;
import pack.iscsi.io.FileIO;
import pack.iscsi.io.IOUtils;
import pack.iscsi.s3.S3TestProperties;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.block.S3BlockReader.S3BlockReaderConfig;
import pack.iscsi.s3.block.S3BlockWriter.S3BlockWriterConfig;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.async.AsyncCompletableFuture;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockMetadata;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.wal.BlockJournalRange;
import pack.iscsi.spi.wal.BlockRecoveryWriter;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

public class S3LocalBlockTest {
  private int _blockSize;
  private int _blockCount;
  private long _seed;
  private File _dir;
  private Random _random;

  @Before
  public void setup() {
    _dir = new File("./target/tmp/S3LocalBlockTest");
    IOUtils.rmr(_dir);
    _dir.mkdirs();

    _blockSize = 2_000_000;
    _blockCount = 1000;
    _seed = new Random().nextLong();
    _random = new Random(_seed);
  }

  @Test
  public void testS3LocalBlock() throws Exception {

    ConsistentAmazonS3 consistentAmazonS3 = S3TestSetup.getConsistentAmazonS3();
    String bucket = S3TestProperties.getBucket();
    String objectPrefix = S3TestProperties.getObjectPrefix();

    S3BlockWriter writer = new S3BlockWriter(S3BlockWriterConfig.builder()
                                                                .bucket(bucket)
                                                                .consistentAmazonS3(consistentAmazonS3)
                                                                .objectPrefix(objectPrefix)
                                                                .build());
    S3BlockReader reader = new S3BlockReader(S3BlockReaderConfig.builder()
                                                                .bucket(bucket)
                                                                .consistentAmazonS3(consistentAmazonS3)
                                                                .objectPrefix(objectPrefix)
                                                                .build());

    BlockStateStore blockStateStore = getBlockStateStore();
    BlockGenerationStore store = getBlockStore();
    BlockWriteAheadLog wal = getBlockWriteAheadLog();

    long volumeId = _random.nextLong();
    long blockId = _random.nextInt(_blockCount);
    long volumeLengthInBytes = _blockSize * _blockCount;

    Random random = new Random(_seed);
    byte[] blockData = new byte[_blockSize];
    try (RandomAccessIO randomAccessIO = getRandomAccessIO(_dir, volumeLengthInBytes)) {
      LocalBlockConfig config = getLocalBlockConfig(volumeId, blockId, _blockSize, randomAccessIO, blockStateStore,
          store, wal);
      random.nextBytes(blockData);
      try (Block block = new LocalBlock(config)) {
        assertTrue(block.idleWrites());
        block.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));
        block.writeFully(0, blockData, 0, blockData.length);
        block.execIO(writer);

        byte[] buffer2 = new byte[_blockSize];
        block.readFully(0, buffer2, 0, buffer2.length);
        assertArrays(blockData, buffer2);
      }
    }
    IOUtils.rmr(_dir);
    _dir.mkdirs();
    try (RandomAccessIO randomAccessIO = getRandomAccessIO(_dir, volumeLengthInBytes)) {
      LocalBlockConfig config = getLocalBlockConfig(volumeId, blockId, _blockSize, randomAccessIO, blockStateStore,
          store, wal);
      byte[] buffer = new byte[_blockSize];
      try (Block block = new LocalBlock(config)) {
        assertTrue(block.idleWrites());
        block.execIO(reader);
        block.readFully(0, buffer, 0, buffer.length);
      }
      assertArrays(blockData, buffer);
    }
  }

  private void assertArrays(byte[] blockData, byte[] buffer2) {
    for (int i = 0; i < blockData.length; i++) {
      assertEquals("offset " + i + " " + blockData[i] + " " + buffer2[i], blockData[i], buffer2[i]);
    }
    assertTrue(Arrays.equals(blockData, buffer2));
  }

  private BlockWriteAheadLog getBlockWriteAheadLog() {
    return new BlockWriteAheadLog() {

      @Override
      public AsyncCompletableFuture write(long volumeId, long blockId, long generation, long position, byte[] bytes,
          int offset, int len) throws IOException {
        return AsyncCompletableFuture.completedFuture();
      }

      @Override
      public void releaseJournals(long volumeId, long blockId, long generation) throws IOException {

      }

      @Override
      public List<BlockJournalRange> getJournalRanges(long volumeId, long blockId, long onDiskGeneration,
          boolean closeExistingWriter) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public long recoverFromJournal(BlockRecoveryWriter writer, BlockJournalRange range, long onDiskGeneration)
          throws IOException {
        throw new RuntimeException("not impl");
      }

    };
  }

  private BlockGenerationStore getBlockStore() {
    return new BlockGenerationStore() {

      private long _lastStoredGeneration;

      @Override
      public long getLastStoredGeneration(long volumeId, long blockId) {
        return _lastStoredGeneration;
      }

      @Override
      public void setLastStoredGeneration(long volumeId, long blockId, long lastStoredGeneration) {
        _lastStoredGeneration = lastStoredGeneration;
      }

      @Override
      public Map<BlockKey, Long> getAllLastStoredGeneration(long volumeId) throws IOException {
        throw new RuntimeException("not impl");
      }
    };
  }

  private RandomAccessIO getRandomAccessIO(File dir, long length) throws IOException {
    int bufferSize = 1024 * 1024;
    File file = new File(dir, UUID.randomUUID()
                                  .toString());
    RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, bufferSize, "rw");
    randomAccessIO.setLength(length);
    return randomAccessIO;
  }

  private BlockStateStore getBlockStateStore() {
    ConcurrentMap<Long, BlockMetadata> metadataMap = new ConcurrentHashMap<>();
    return new BlockStateStore() {

      @Override
      public void setBlockMetadata(long volumeId, long blockId, BlockMetadata metadata) throws IOException {
        metadataMap.put(blockId, metadata);
      }

      @Override
      public void removeBlockMetadata(long volumeId, long blockId) throws IOException {
        metadataMap.remove(blockId);
      }

      @Override
      public BlockMetadata getBlockMetadata(long volumeId, long blockId) throws IOException {
        return metadataMap.get(blockId);
      }

      @Override
      public void setMaxBlockCount(long volumeId, long blockCount) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void createBlockMetadataStore(long volumeId) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public void destroyBlockMetadataStore(long volumeId) throws IOException {
        throw new RuntimeException("not impl");
      }
    };
  }

  private LocalBlockConfig getLocalBlockConfig(long volumeId, long blockId, int blockSize,
      RandomAccessIO randomAccessIO, BlockStateStore blockStateStore, BlockGenerationStore store,
      BlockWriteAheadLog wal) {

    LocalBlockConfig config = LocalBlockConfig.builder()
                                              .randomAccessIO(randomAccessIO)
                                              .blockStateStore(blockStateStore)
                                              .volumeId(volumeId)
                                              .blockId(blockId)
                                              .blockGenerationStore(store)
                                              .blockSize(blockSize)
                                              .wal(wal)
                                              .build();
    return config;
  }

}
