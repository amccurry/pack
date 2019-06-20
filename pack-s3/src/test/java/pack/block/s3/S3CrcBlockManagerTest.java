package pack.block.s3;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.UUID;

import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import pack.block.BlockConfig;
import pack.block.CrcBlockManager;
import pack.block.s3.S3Block;
import pack.block.s3.S3BlockConfig;
import pack.block.util.CRC64;

public class S3CrcBlockManagerTest {

  private static final File ROOT = new File("./target/tmp/CrcBlockManagerTest");

  @Test
  public void testCrcBlockManager() throws Exception {
    Utils.rmr(ROOT);
    ROOT.mkdirs();
    long blockSize = 10_000_000;
    CrcBlockManager crcBlockManager = InMemoryCrcBlockManager.newInstance();
    String volume = "CrcBlockManagerTest";
    BlockConfig blockConfig = BlockConfig.builder()
                                         .blockId(Long.MAX_VALUE)
                                         .blockSize(blockSize)
                                         .crcBlockManager(crcBlockManager)
                                         .volume(volume)
                                         .build();
    String bucketName = "pack-s3-testing";
    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    File localCacheFile = new File(ROOT, "cache");
    String prefix = "testCrcBlockManager-" + UUID.randomUUID()
                                                 .toString();
    S3BlockConfig config = S3BlockConfig.builder()
                                        .blockConfig(blockConfig)
                                        .bucketName(bucketName)
                                        .client(client)
                                        .localCacheFile(localCacheFile)
                                        .prefix(prefix)
                                        .build();
    try (CrcBlockManager manager = CrcBlockManager.create(new S3Block(config))) {
      manager.putBlockCrc(0, 1);
      assertEquals(1, manager.getBlockCrc(0));
      assertEquals(CRC64.DEFAULT_VALUE, manager.getBlockCrc(10));
    }

    try (CrcBlockManager manager = CrcBlockManager.create(new S3Block(config))) {
      assertEquals(1, manager.getBlockCrc(0));
      assertEquals(CRC64.DEFAULT_VALUE, manager.getBlockCrc(10));
    }
  }

}
