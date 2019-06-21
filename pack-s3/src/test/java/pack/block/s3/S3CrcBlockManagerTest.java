package pack.block.s3;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.UUID;

import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import pack.block.CrcBlockManager;
import pack.block.util.CRC64;

public class S3CrcBlockManagerTest {

  private static final File ROOT = new File("./target/tmp/S3CrcBlockManagerTest");

  @Test
  public void testCrcBlockManager() throws Exception {
    Utils.rmr(ROOT);
    ROOT.mkdirs();
    // long blockSize = 10_000_000;
    // CrcBlockManager crcBlockManager = InMemoryCrcBlockManager.newInstance();
    // String volume = "CrcBlockManagerTest";
    // BlockConfig blockConfig = BlockConfig.builder()
    // .blockId(Long.MAX_VALUE)
    // .blockSize(blockSize)
    // .crcBlockManager(crcBlockManager)
    // .volume(volume)
    // .build();
    // String bucketName = "pack-s3-testing";
    // AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    // File localCacheFile = new File(ROOT, "cache");
    // String prefix = "testCrcBlockManager-" + UUID.randomUUID()
    // .toString();
    // S3BlockConfig config = S3BlockConfig.builder()
    // .blockConfig(blockConfig)
    // .bucketName(bucketName)
    // .client(client)
    // .localCacheFile(localCacheFile)
    // .prefix(prefix)
    // .build();

    String volume = "S3CrcBlockManagerTest";

    CrcBlockManager crcBlockManager = InMemoryCrcBlockManager.newInstance();

    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    String bucketName = "pack-s3-testing";
    String prefix = "testS3CrcBlockManagerTest-" + UUID.randomUUID()
                                                       .toString();

    S3CrcBlockManagerConfig config = S3CrcBlockManagerConfig.builder()
                                                            .volume(volume)
                                                            .crcBlockManager(crcBlockManager)
                                                            .bucketName(bucketName)
                                                            .client(client)
                                                            .prefix(prefix)
                                                            .build();

    try (CrcBlockManager manager = new S3CrcBlockManager(config)) {
      manager.putBlockCrc(0, 1);
      assertEquals(1, manager.getBlockCrc(0));
      assertEquals(CRC64.DEFAULT_VALUE, manager.getBlockCrc(10));
    }

    try (CrcBlockManager manager = new S3CrcBlockManager(config)) {
      assertEquals(1, manager.getBlockCrc(0));
      assertEquals(CRC64.DEFAULT_VALUE, manager.getBlockCrc(10));
    }
  }

}
