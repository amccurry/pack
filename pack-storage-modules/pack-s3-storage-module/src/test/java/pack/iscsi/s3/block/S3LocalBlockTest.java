package pack.iscsi.s3.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.block.LocalBlock;
import pack.iscsi.block.LocalBlockConfig;
import pack.iscsi.io.IOUtils;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.TestProperties;
import pack.iscsi.s3.block.S3BlockReader.S3BlockReaderConfig;
import pack.iscsi.s3.block.S3BlockWriter.S3BlockWriterConfig;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.volume.VolumeMetadata;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.spi.wal.BlockWriteAheadLogResult;

public class S3LocalBlockTest {

  @Test
  public void testS3LocalBlock() throws Exception {
    File file = new File("./target/tmp/S3LocalBlockTest");
    IOUtils.rmr(file);
    file.getParentFile()
        .mkdirs();

    long volumeId = 0;
    long blockId = 0;
    int blockSize = 20_000_000;
    BlockGenerationStore store = getBlockStore();
    BlockWriteAheadLog wal = getBlockWriteAheadLog();
    LocalBlockConfig config = LocalBlockConfig.builder()
                                              .blockDataDir(file)
                                              .volumeMetadata(getVolumeMetadata(volumeId, blockSize))
                                              .blockId(blockId)
                                              .blockStore(store)
                                              .wal(wal)
                                              .build();

    ConsistentAmazonS3 consistentAmazonS3 = S3TestSetup.getConsistentAmazonS3();
    String bucket = TestProperties.getBucket();
    String objectPrefix = TestProperties.getObjectPrefix();

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

    Random random = new Random();
    byte[] blockData = new byte[blockSize];
    random.nextBytes(blockData);
    try (Block block = new LocalBlock(config)) {
      assertTrue(block.idleWrites());
      block.execIO(request -> BlockIOResponse.newBlockIOResult(0, BlockState.CLEAN, 0));
      block.writeFully(0, blockData, 0, blockSize);
      block.execIO(writer);

      byte[] buffer2 = new byte[blockSize];
      block.readFully(0, buffer2, 0, blockSize);
      assertArrays(blockData, buffer2);
    }
    IOUtils.rmr(file);
    file.getParentFile()
        .mkdirs();
    byte[] buffer = new byte[blockSize];
    try (Block block = new LocalBlock(config)) {
      assertTrue(block.idleWrites());
      block.execIO(reader);
      block.readFully(0, buffer, 0, blockSize);
    }
    assertArrays(blockData, buffer);
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
      public BlockWriteAheadLogResult write(long volumeId, long blockId, long generation, long position, byte[] bytes,
          int offset, int len) throws IOException {
        return () -> {

        };
      }

      @Override
      public void release(long volumeId, long blockId, long generation) throws IOException {

      }

      @Override
      public long recover(RandomAccessIO randomAccessIO, long volumeId, long blockId, long onDiskGeneration)
          throws IOException {
        throw new RuntimeException("not impl");
      }

    };
  }

  private BlockGenerationStore getBlockStore() {
    return new BlockGenerationStore() {

      private long _lastStoredGeneration;

      @Override
      public long getLastStoreGeneration(long volumeId, long blockId) {
        return _lastStoredGeneration;
      }

      @Override
      public void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) {
        _lastStoredGeneration = lastStoredGeneration;
      }
    };
  }

  private VolumeMetadata getVolumeMetadata(long volumeId, int blockSize) {
    return VolumeMetadata.builder()
                         .blockSize(blockSize)
                         .volumeId(volumeId)
                         .build();
  }
}
