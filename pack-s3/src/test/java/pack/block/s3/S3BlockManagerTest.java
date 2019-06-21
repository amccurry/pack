package pack.block.s3;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import pack.block.BlockFactory;
import pack.block.BlockManager;
import pack.block.BlockManagerConfig;
import pack.block.CrcBlockManager;

public class S3BlockManagerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BlockManagerTest.class);
  private static final File ROOT = new File("./target/tmp/S3BlockManagerTest");
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
  public void testS3BlockManager() throws Exception {
    String prefix = "S3BlockManagerTest";
    S3BlockFactoryConfig s3FactoryConfig = S3BlockFactoryConfig.builder()
                                                               .bucketName(BUCKET)
                                                               .cacheDir(ROOT)
                                                               .client(AmazonS3ClientBuilder.defaultClient())
                                                               .prefix(prefix)
                                                               .build();
    BlockFactory blockFactory = new S3BlockFactory(s3FactoryConfig);
    long blockSize = 1_000_000;
    long cacheSize = 20_000_000;
    long idleWriteTime = TimeUnit.SECONDS.toMillis(10);
    CrcBlockManager crcBlockManager = InMemoryCrcBlockManager.newInstance();
    BlockManagerConfig config = BlockManagerConfig.builder()
                                                  .volume("test")
                                                  .blockFactory(blockFactory)
                                                  .blockSize(blockSize)
                                                  .cacheSize(cacheSize)
                                                  .crcBlockManager(crcBlockManager)
                                                  .idleWriteTime(idleWriteTime)
                                                  .build();

    int maxWriteBuffer = 200000;
    int minWriteBuffer = 100;
    int maxPosition = 200_000_000;

    long seed = 1;
    int passes = 100;
    File file = new File(ROOT, "validation");
    Utils.rmr(file);

    Random random = new Random(seed);

    try (RandomAccessFile rand = new RandomAccessFile(file, "rw")) {
      try (BlockManager block = new BlockManager(config)) {
        for (int pass = 0; pass < passes; pass++) {
          int bufferLength = random.nextInt(maxWriteBuffer - minWriteBuffer) + minWriteBuffer;
          byte[] buffer = new byte[bufferLength];
          random.nextBytes(buffer);
          int pos = random.nextInt(maxPosition);

          LOGGER.info("Writing at pos {} len {}", pos, bufferLength);
          block.writeFully(pos, buffer, 0, bufferLength);
          growFile(rand, pos + bufferLength);
          rand.seek(pos);
          rand.write(buffer, 0, bufferLength);
        }
      }
    }
    
    Thread.sleep(10000);

    System.out.println("--------------------------------------");

    try (RandomAccessFile rand = new RandomAccessFile(file, "r")) {
      try (BlockManager block = new BlockManager(config)) {
        validateData(rand, block);
      }
    }
  }

  private void validateData(RandomAccessFile rand, BlockManager block) throws IOException, Exception {
    byte[] buf1 = new byte[1024];
    byte[] buf2 = new byte[1024];

    int pos = 0;
    int length = (int) rand.length();
    while (pos < length) {
      Arrays.fill(buf1, (byte) 0);
      Arrays.fill(buf2, (byte) 0);
      int len = Math.min(buf1.length, length);

      rand.seek(pos);
      int read = rand.read(buf1, 0, len);

      block.readFully(pos, buf2, 0, read);

      pos += read;
      boolean equals = Arrays.equals(buf1, buf2);
      if (!equals) {
        System.out.println(pos);
      }
      assertTrue(equals);
    }
  }

  private void growFile(RandomAccessFile rand, int length) throws IOException {
    if (rand.length() < length) {
      rand.setLength(length);
    }
  }
}
