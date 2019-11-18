package pack.iscsi.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import pack.iscsi.io.FileIO;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.async.AsyncCompletableFuture;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockMetadata;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.block.BlockStateStore;

public class LocalBlockTest {

  private int _blockSize;
  private int _blockCount;
  private int _passes;
  private long _seed;
  private File _dir;
  private Random _random;

  @Before
  public void setup() {
    _dir = new File("./target/tmp/LocalBlockTest");
    IOUtils.rmr(_dir);
    _dir.mkdirs();

    _blockSize = 2_000_000;
    _blockCount = 1000;
    _passes = 1000;
    _seed = new Random().nextLong();
    _random = new Random(_seed);
  }

  @Test
  public void testBlockSimple() throws IOException, InterruptedException, ExecutionException {
    long volumeId = _random.nextLong();
    long blockId = _random.nextInt(_blockCount);

    long volumeLengthInBytes = _blockSize * _blockCount;

    try (RandomAccessIO randomAccessIO = getRandomAccessIO(_dir, volumeLengthInBytes)) {
      LocalBlockConfig config = getLocalBlockConfig(volumeId, blockId, _blockSize, randomAccessIO);
      try (Block block = new LocalBlock(config)) {
        block.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));
        {
          byte[] buffer1 = new byte[1000];
          byte[] buffer2 = new byte[1000];
          Random random = new Random(_seed);

          for (int i = 0; i < _passes; i++) {
            long blockPosition = i * buffer1.length;
            random.nextBytes(buffer1);
            AsyncCompletableFuture future = block.writeFully(blockPosition, buffer1, 0, buffer1.length);
            future.get();
            block.readFully(blockPosition, buffer2, 0, buffer2.length);
            assertTrue(Arrays.equals(buffer1, buffer2));
          }
        }
        {
          byte[] buffer1 = new byte[1000];
          byte[] buffer2 = new byte[1000];
          Random random = new Random(_seed);

          for (int i = 0; i < _passes; i++) {
            long blockPosition = i * buffer1.length;
            random.nextBytes(buffer1);
            block.readFully(blockPosition, buffer2, 0, buffer2.length);
            assertTrue(Arrays.equals(buffer1, buffer2));
          }
        }

        assertEquals(1000, block.getOnDiskGeneration());
        assertEquals(BlockState.DIRTY, block.getOnDiskState());
        assertEquals(0, block.getLastStoredGeneration());
      }
    }
  }

  @Test
  public void testBlockWritePastEndOfBlock() throws IOException {
    long volumeId = _random.nextLong();
    long blockId = _random.nextInt(_blockCount);
    long volumeLengthInBytes = _blockSize * _blockCount;
    try (RandomAccessIO randomAccessIO = getRandomAccessIO(_dir, volumeLengthInBytes)) {
      LocalBlockConfig config = getLocalBlockConfig(volumeId, blockId, _blockSize, randomAccessIO);
      try (Block block = new LocalBlock(config)) {
        block.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));
        byte[] buffer = new byte[1000];
        try {
          block.writeFully(_blockSize, buffer, 0, 1);
          fail();
        } catch (EOFException e) {

        }
      }
    }
  }

  @Test
  public void testBlockWriteWithStore() throws IOException {
    long volumeId = _random.nextLong();
    long blockId = _random.nextInt(_blockCount);
    long volumeLengthInBytes = _blockSize * _blockCount;
    try (RandomAccessIO randomAccessIO = getRandomAccessIO(_dir, volumeLengthInBytes)) {
      LocalBlockConfig config = getLocalBlockConfig(volumeId, blockId, _blockSize, randomAccessIO);
      try (Block block = new LocalBlock(config)) {
        block.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));
        byte[] buffer = new byte[1000];
        block.writeFully(0, buffer, 0, 1);

        assertEquals(1, block.getOnDiskGeneration());
        assertEquals(BlockState.DIRTY, block.getOnDiskState());
        assertEquals(0, block.getLastStoredGeneration());

        block.execIO(request -> BlockIOResponse.newBlockIOResult(request.getOnDiskGeneration(), BlockState.CLEAN,
            request.getOnDiskGeneration()));

        assertEquals(1, block.getOnDiskGeneration());
        assertEquals(BlockState.CLEAN, block.getOnDiskState());
        assertEquals(1, block.getLastStoredGeneration());
      }
    }
  }

  @Test
  public void testBlockWriteWithStoreWithFailure() throws IOException {
    long volumeId = _random.nextLong();
    long blockId = _random.nextInt(_blockCount);
    long volumeLengthInBytes = _blockSize * _blockCount;
    try (RandomAccessIO randomAccessIO = getRandomAccessIO(_dir, volumeLengthInBytes)) {
      LocalBlockConfig config = getLocalBlockConfig(volumeId, blockId, _blockSize, randomAccessIO);
      try (Block block = new LocalBlock(config)) {
        block.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));
        byte[] buffer = new byte[1000];
        block.writeFully(0, buffer, 0, 1);

        assertEquals(1, block.getOnDiskGeneration());
        assertEquals(BlockState.DIRTY, block.getOnDiskState());
        assertEquals(0, block.getLastStoredGeneration());

        try {
          block.execIO(request -> {
            throw new RuntimeException();
          });
        } catch (Exception e) {

        }

        assertEquals(1, block.getOnDiskGeneration());
        assertEquals(BlockState.DIRTY, block.getOnDiskState());
        assertEquals(0, block.getLastStoredGeneration());
      }
    }
  }

  @Test
  public void testBlockWriteWithStoreIdleWriteCheck() throws IOException, InterruptedException {
    long volumeId = _random.nextLong();
    long blockId = _random.nextInt(_blockCount);
    long volumeLengthInBytes = _blockSize * _blockCount;
    try (RandomAccessIO randomAccessIO = getRandomAccessIO(_dir, volumeLengthInBytes)) {
      LocalBlockConfig config = getLocalBlockConfig(volumeId, blockId, _blockSize, randomAccessIO);
      config = config.toBuilder()
                     .syncTimeAfterIdle(1)
                     .syncTimeAfterIdleTimeUnit(TimeUnit.SECONDS)
                     .build();

      try (Block block = new LocalBlock(config)) {
        assertTrue(block.idleWrites());
        block.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));
        byte[] buffer = new byte[1000];
        block.writeFully(0, buffer, 0, 1);
        assertFalse(block.idleWrites());
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        assertTrue(block.idleWrites());
      }
    }
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

  private LocalBlockConfig getLocalBlockConfig(long volumeId, long blockId, int blockSize,
      RandomAccessIO randomAccessIO) {
    BlockStateStore blockStateStore = getBlockStateStore();
    BlockGenerationStore store = getBlockStore();
    LocalBlockConfig config = LocalBlockConfig.builder()
                                              .randomAccessIO(randomAccessIO)
                                              .blockStateStore(blockStateStore)
                                              .volumeId(volumeId)
                                              .blockId(blockId)
                                              .blockGenerationStore(store)
                                              .blockSize(blockSize)
                                              .build();
    return config;
  }

  private RandomAccessIO getRandomAccessIO(File dir, long length) throws IOException {
    File file = new File(dir, UUID.randomUUID()
                                  .toString());
    RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, _blockSize, "rw");
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
}
