package pack.iscsi.external.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import com.amazonaws.services.s3.model.S3Object;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.S3TestSetup;
import pack.iscsi.TestProperties;
import pack.iscsi.external.s3.S3BlockWriter;
import pack.iscsi.partitioned.block.BlockIORequest;
import pack.iscsi.partitioned.block.BlockIOResponse;
import pack.iscsi.partitioned.block.BlockState;

public class S3BlockWriterTest {

  @Test
  public void testS3BlockWriter() throws Exception {
    ConsistentAmazonS3 consistentAmazonS3 = S3TestSetup.getConsistentAmazonS3();
    String bucket = TestProperties.getBucket();
    String objectPrefix = TestProperties.getObjectPrefix();
    S3BlockWriter writer = new S3BlockWriter(consistentAmazonS3, bucket, objectPrefix);
    File file = new File("./target/tmp/S3BlockWriterTest/test");
    file.getParentFile()
        .mkdirs();
    byte[] buffer = new byte[10_000];
    Random random = new Random();
    random.nextBytes(buffer);
    try (FileOutputStream output = new FileOutputStream(file)) {
      output.write(buffer);
    }
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      try (FileChannel channel = raf.getChannel()) {
        int blockSize = 9_000;
        BlockIORequest request = BlockIORequest.builder()
                                               .blockSize(blockSize)
                                               .onDiskGeneration(1)
                                               .lastStoredGeneration(0)
                                               .onDiskState(BlockState.DIRTY)
                                               .channel(channel)
                                               .build();

        BlockIOResponse response = writer.exec(request);
        assertEquals(BlockState.CLEAN, response.getOnDiskBlockState());
        assertEquals(1, response.getOnDiskGeneration());
        assertEquals(1, response.getLastStoredGeneration());

        String key = "0/0/1";
        S3Object object = consistentAmazonS3.getObject(bucket, key);
        assertEquals(blockSize, object.getObjectMetadata()
                                      .getContentLength());
        byte[] buffer2 = new byte[blockSize];
        try (DataInputStream dataInput = new DataInputStream(object.getObjectContent())) {
          dataInput.readFully(buffer2);
        }

        assertTrue(Arrays.equals(getPartial(buffer, blockSize), buffer2));

      }
    }
  }

  private byte[] getPartial(byte[] buffer, int blockSize) {
    byte[] buf = new byte[blockSize];
    System.arraycopy(buffer, 0, buf, 0, blockSize);
    return buf;
  }

}
