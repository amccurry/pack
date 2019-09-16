package pack.iscsi.s3.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.io.FileIO;
import pack.iscsi.s3.S3TestSetup;
import pack.iscsi.s3.TestProperties;
import pack.iscsi.s3.block.S3BlockReader.S3BlockReaderConfig;
import pack.iscsi.s3.block.S3BlockWriter.S3BlockWriterConfig;
import pack.iscsi.spi.block.BlockIORequest;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;

public class S3BlockReaderWriterTest {

  @Test
  public void testS3BlockWriter() throws Exception {
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
    File file1 = new File("./target/tmp/S3BlockWriterTest/test-" + UUID.randomUUID()
                                                                       .toString());
    file1.getParentFile()
         .mkdirs();

    File file2 = new File("./target/tmp/S3BlockWriterTest/test-" + UUID.randomUUID()
                                                                       .toString());
    byte[] buffer = new byte[10_000];
    Random random = new Random();
    random.nextBytes(buffer);
    try (FileOutputStream output = new FileOutputStream(file1)) {
      output.write(buffer);
    }
    int blockSize = 9_000;
    try (FileIO fileIO = FileIO.open(file1, 4096, file1.length())) {
      BlockIORequest request = BlockIORequest.builder()
                                             .blockSize(blockSize)
                                             .onDiskGeneration(1)
                                             .lastStoredGeneration(0)
                                             .onDiskState(BlockState.DIRTY)
                                             .randomAccessIO(fileIO)
                                             .build();

      BlockIOResponse response = writer.exec(request);
      assertEquals(BlockState.CLEAN, response.getOnDiskBlockState());
      assertEquals(1, response.getOnDiskGeneration());
      assertEquals(1, response.getLastStoredGeneration());
    }

    try (FileIO fileIO = FileIO.open(file2, 4096, file2.length())) {
      BlockIORequest request = BlockIORequest.builder()
                                             .blockSize(blockSize)
                                             .onDiskGeneration(0)
                                             .lastStoredGeneration(1)
                                             .onDiskState(BlockState.CLEAN)
                                             .randomAccessIO(fileIO)
                                             .build();
      BlockIOResponse response = reader.exec(request);
      assertEquals(BlockState.CLEAN, response.getOnDiskBlockState());
      assertEquals(1, response.getOnDiskGeneration());
      assertEquals(1, response.getLastStoredGeneration());

      byte[] buffer2 = new byte[blockSize];
      fileIO.readFully(0, buffer2);
      assertTrue(Arrays.equals(getPartial(buffer, blockSize), buffer2));
    }
    assertEquals(blockSize, file2.length());
  }

  private byte[] getPartial(byte[] buffer, int blockSize) {
    byte[] buf = new byte[blockSize];
    System.arraycopy(buffer, 0, buf, 0, blockSize);
    return buf;
  }

}
