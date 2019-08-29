package pack.iscsi.partitioned.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
import pack.util.IOUtils;

public class LocalBlockTest {

  @Test
  public void testBlockSimple() throws IOException {
    File file = new File("./target/tmp/LocalBlockTest");
    IOUtils.rmr(file);
    file.getParentFile()
        .mkdirs();

    long volumeId = 0;
    long blockId = 0;
    int blockSize = 20_000_000;
    BlockStore store = getBlockStore();
    BlockWriteAheadLog wal = getBlockWriteAheadLog();
    long seed = new Random().nextLong();

    int passes = 1000;
    LocalBlockConfig config = LocalBlockConfig.builder()
                                              .blockDataDir(file)
                                              .volumeId(volumeId)
                                              .blockId(blockId)
                                              .blockSize(blockSize)
                                              .blockStore(store)
                                              .wal(wal)
                                              .build();
    try (Block block = new LocalBlock(config)) {

      block.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));

      {
        byte[] buffer1 = new byte[1000];
        byte[] buffer2 = new byte[1000];
        Random random = new Random(seed);

        for (int i = 0; i < passes; i++) {
          long blockPosition = i * buffer1.length;
          random.nextBytes(buffer1);
          block.writeFully(blockPosition, buffer1, 0, buffer1.length);
          block.readFully(blockPosition, buffer2, 0, buffer2.length);
          assertTrue(Arrays.equals(buffer1, buffer2));
        }
      }
      {
        byte[] buffer1 = new byte[1000];
        byte[] buffer2 = new byte[1000];
        Random random = new Random(seed);

        for (int i = 0; i < passes; i++) {
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

  @Test
  public void testBlockWritePastEndOfBlock() throws IOException {
    File file = new File("./target/tmp/LocalBlockTest");
    IOUtils.rmr(file);
    file.getParentFile()
        .mkdirs();

    long volumeId = 0;
    long blockId = 0;
    int blockSize = 20_000_000;
    BlockStore store = getBlockStore();
    BlockWriteAheadLog wal = getBlockWriteAheadLog();
    LocalBlockConfig config = LocalBlockConfig.builder()
                                              .blockDataDir(file)
                                              .volumeId(volumeId)
                                              .blockId(blockId)
                                              .blockSize(blockSize)
                                              .blockStore(store)
                                              .wal(wal)
                                              .build();
    try (Block block = new LocalBlock(config)) {
      block.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));
      byte[] buffer = new byte[1000];
      try {
        block.writeFully(blockSize, buffer, 0, 1);
        fail();
      } catch (EOFException e) {

      }
    }
  }

  @Test
  public void testBlockWriteWithStore() throws IOException {
    File file = new File("./target/tmp/LocalBlockTest");
    IOUtils.rmr(file);
    file.getParentFile()
        .mkdirs();

    long volumeId = 0;
    long blockId = 0;
    int blockSize = 20_000_000;
    BlockStore store = getBlockStore();
    BlockWriteAheadLog wal = getBlockWriteAheadLog();
    LocalBlockConfig config = LocalBlockConfig.builder()
                                              .blockDataDir(file)
                                              .volumeId(volumeId)
                                              .blockId(blockId)
                                              .blockSize(blockSize)
                                              .blockStore(store)
                                              .wal(wal)
                                              .build();
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

  @Test
  public void testBlockWriteWithStoreWithFailure() throws IOException {
    File file = new File("./target/tmp/LocalBlockTest");
    IOUtils.rmr(file);
    file.getParentFile()
        .mkdirs();

    long volumeId = 0;
    long blockId = 0;
    int blockSize = 20_000_000;
    BlockStore store = getBlockStore();
    BlockWriteAheadLog wal = getBlockWriteAheadLog();
    LocalBlockConfig config = LocalBlockConfig.builder()
                                              .blockDataDir(file)
                                              .volumeId(volumeId)
                                              .blockId(blockId)
                                              .blockSize(blockSize)
                                              .blockStore(store)
                                              .wal(wal)
                                              .build();
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

  @Test
  public void testBlockWriteWithStoreIdleWriteCheck() throws IOException, InterruptedException {
    File file = new File("./target/tmp/LocalBlockTest");
    IOUtils.rmr(file);
    file.getParentFile()
        .mkdirs();

    long volumeId = 0;
    long blockId = 0;
    int blockSize = 20_000_000;
    BlockStore store = getBlockStore();
    BlockWriteAheadLog wal = getBlockWriteAheadLog();
    LocalBlockConfig config = LocalBlockConfig.builder()
                                              .blockDataDir(file)
                                              .volumeId(volumeId)
                                              .blockId(blockId)
                                              .blockSize(blockSize)
                                              .blockStore(store)
                                              .wal(wal)
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

  private BlockWriteAheadLog getBlockWriteAheadLog() {
    return new BlockWriteAheadLog() {

      @Override
      public void write(long volumeId, long blockId, long generation, long position, byte[] bytes, int offset, int len)
          throws IOException {

      }

      @Override
      public BlockIOExecutor getWriteAheadLogReader() {
        throw new RuntimeException("not impl");
      }

      @Override
      public void release(long volumeId, long blockId, long generation) throws IOException {
      }
    };
  }

  private BlockStore getBlockStore() {
    return new BlockStore() {

      private long _lastStoredGeneration;

      @Override
      public long getLastStoreGeneration(long volumeId, long blockId) {
        return _lastStoredGeneration;
      }

      @Override
      public void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) {
        _lastStoredGeneration = lastStoredGeneration;
      }

      @Override
      public List<String> getVolumeNames() {
        throw new RuntimeException("not impl");
      }

      @Override
      public long getVolumeId(String name) {
        throw new RuntimeException("not impl");
      }

      @Override
      public long getLengthInBytes(long volumeId) {
        throw new RuntimeException("not impl");
      }

      @Override
      public int getBlockSize(long volumeId) {
        throw new RuntimeException("not impl");
      }
    };
  }
}
