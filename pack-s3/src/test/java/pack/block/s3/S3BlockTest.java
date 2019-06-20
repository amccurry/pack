package pack.block.s3;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import pack.block.Block;
import pack.block.BlockConfig;
import pack.block.CrcBlockManager;

public class S3BlockTest {

  private static final File ROOT = new File("./target/tmp/S3BlockTest");
  private static final String BUCKET = "pack-s3-testing";

  @Before
  public void setup() {
    Utils.rmr(ROOT);
    ROOT.mkdirs();

    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    ObjectListing listObjects = client.listObjects(BUCKET);
    List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
    for (S3ObjectSummary objectSummary : objectSummaries) {
      client.deleteObject(BUCKET, objectSummary.getKey());
    }
  }

  @Test
  public void testS3Block() throws Exception {
    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();

    File file = new File(ROOT, "./test-" + UUID.randomUUID()
                                               .toString());
    int blockId = 1001;
    int blockSize = 10_000_000;
    String prefix = "S3BlockTest";

    CrcBlockManager crcBlockManager = InMemoryCrcBlockManager.newInstance();
    BlockConfig blockConfig = BlockConfig.builder()
                                         .blockId(blockId)
                                         .blockSize(blockSize)
                                         .crcBlockManager(crcBlockManager)
                                         .volume("test")
                                         .build();

    S3BlockConfig config = S3BlockConfig.builder()
                                        .blockConfig(blockConfig)
                                        .bucketName(BUCKET)
                                        .prefix(prefix)
                                        .client(client)
                                        .localCacheFile(file)
                                        .build();

    Random random = new Random();
    byte val = (byte) random.nextInt();

    try (Block block = new S3Block(config)) {
      {
        byte[] array = new byte[100];
        Arrays.fill(array, val);
        block.write(100, array, 0, 100);
      }

      {
        byte[] array = new byte[100];
        block.read(100, array, 0, 100);
        for (int i = 0; i < 100; i++) {
          assertEquals(val, array[i]);
        }
      }

      block.sync();
    }

    try (Block block = new S3Block(config)) {
      byte[] array = new byte[blockId];
      block.read(100, array, 0, 100);
      for (int i = 0; i < 100; i++) {
        assertEquals(val, array[i]);
      }
    }
  }

}
