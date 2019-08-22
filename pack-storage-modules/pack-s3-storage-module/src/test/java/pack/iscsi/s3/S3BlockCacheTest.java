package pack.iscsi.s3;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class S3BlockCacheTest {

  @Test
  public void testS3BlockCacheSimple() throws IOException {
    File file = new File("./target/tmp/S3BlockCacheTest");
    if (file.exists()) {
      file.delete();
    } else {
      file.getParentFile()
          .mkdirs();
    }
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      int blocks = 100000;
      int passes = 10000;
      int blockSize = 1000;
      long length = S3Block.newFileLength(blocks, blockSize);
      raf.setLength(length);

      int seed = 1;
      {
        Random random = new Random(seed);
        for (int blockId = 0; blockId < passes; blockId++) {
          byte[] buffer1 = new byte[blockSize];
          byte[] buffer2 = new byte[blockSize];
          try (S3Block cache = new S3Block(raf.getChannel(), blockId, blockSize)) {
            cache.exec((channel, positionOfStartOfBlock, blockSize1) -> {
              S3BlockState state = cache.getState();
              if (state == null || state == S3BlockState.MISSING) {
                return S3BlockState.CLEAN;
              }
              return state;
            });
            random.nextBytes(buffer1);
            cache.writeFully(0, buffer1, 0, buffer1.length);

            cache.readFully(0, buffer2, 0, buffer2.length);
            assertTrue(Arrays.equals(buffer1, buffer2));
          }
        }
      }
      {
        Random random = new Random(seed);
        for (int blockId = 0; blockId < passes; blockId++) {
          byte[] buffer1 = new byte[blockSize];
          byte[] buffer2 = new byte[blockSize];
          try (S3Block cache = new S3Block(raf.getChannel(), blockId, blockSize)) {
            random.nextBytes(buffer1);
            cache.readFully(0, buffer2, 0, buffer2.length);
            assertTrue(Arrays.equals(buffer1, buffer2));
          }
        }
      }
    }
  }

}
