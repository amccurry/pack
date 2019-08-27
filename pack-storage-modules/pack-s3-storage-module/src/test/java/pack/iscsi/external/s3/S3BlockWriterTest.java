package pack.iscsi.external.s3;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;

import org.junit.Test;

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
    try (FileOutputStream output = new FileOutputStream(file)) {
      output.write(0);
    }
    BlockIORequest request = BlockIORequest.builder()
                                           .onDiskGeneration(1)
                                           .lastStoredGeneration(0)
                                           .onDiskState(BlockState.DIRTY)
                                           .fileForReadingOnly(file)
                                           .build();

    BlockIOResponse response = writer.exec(request);
    assertEquals(BlockState.CLEAN, response.getOnDiskBlockState());
    assertEquals(1, response.getOnDiskGeneration());
    assertEquals(1, response.getLastStoredGeneration());
  }

}
