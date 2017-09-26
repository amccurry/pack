package pack.block.blockstore.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import pack.block.util.Utils;

public class FileBlockStoreTest {

  private static File storePathDir = new File("./target/FileBlockStoreTest");

  @Before
  public void setup() {
    Utils.rmr(storePathDir);
  }

  @Test
  public void testServer() throws Exception {

    Random random = new Random(1);

    int blockSize = 4096;
    File file = new File(storePathDir, "testServer");
    try (FileBlockStore store = new FileBlockStore(file, 100000000, false)) {
      long s = System.nanoTime();
      for (int i = 0; i < 10000; i++) {

        int pos = random.nextInt(1000) * blockSize;
        {
          byte[] buf = new byte[blockSize];
          store.read(pos, buf, 0, blockSize);
        }
        byte[] data = new byte[blockSize];
        random.nextBytes(data);
        {
          store.write(pos, data, 0, blockSize);
        }
        {
          byte[] buf = new byte[blockSize];
          store.read(pos, buf, 0, blockSize);
          // System.out.println(pos);
          assertTrue(Arrays.equals(data, buf));
        }
      }
      long e = System.nanoTime();
      System.out.println("Run time " + (e - s) / 1_000_000.0 + " ms");
    }
  }

  @Test
  public void testDeleteBlocks() throws Exception {
    File file = new File(storePathDir, "testDeleteBlocks");
    int blockSize = 1024;
    byte[] buf = new byte[blockSize];
    Random random = new Random(1);
    random.nextBytes(buf);

    try (FileBlockStore store = new FileBlockStore(file, 100000000, false)) {
      assertEquals(0, store.getNumberOfBlocksOnDisk());
      store.write(0, buf, 0, blockSize);
      assertTrue(store.getNumberOfBlocksOnDisk() > 0);
      store.delete(0, 100000000);
      assertEquals(0, store.getNumberOfBlocksOnDisk());
    }
  }

  @Test
  public void testReadingWritingNotBlockAligned() throws Exception {
    Random randomSeed = new Random();
    long seed = randomSeed.nextLong();

    Random random = new Random(seed);

    int blockSize = 4096;
    int maxLength = blockSize * 10;
    int maxPos = blockSize * 100;
    int maxOffset = 100;

    File file = new File(storePathDir, "testReadingWritingNotBlockAligned");
    try (FileBlockStore store = new FileBlockStore(file, 100000000, false)) {
      for (int j = 0; j < 10000; j++) {
        int length = random.nextInt(maxLength);
        int offset = random.nextInt(maxOffset);
        byte[] buf1 = new byte[offset + length];

        random.nextBytes(buf1);

        long pos = random.nextInt(maxPos) + 1;

        int writeLength = getLength(pos, length, blockSize);
        long p = pos;
        int off = offset;
        while (writeLength > 0) {
          int write = store.write(p, buf1, off, writeLength);
          writeLength -= write;
          off += write;
          p += write;
        }

        int length2 = length + 2;
        byte[] buf2 = new byte[length2];

        long pos2 = pos - 1;
        assertEquals("Seed " + seed, length2, store.read(pos2, buf2, 0, length2));

        for (int i = 1; i < length2 && i < writeLength + 1; i++) {
          byte expected = buf1[i - 1 + offset];
          byte actual = buf2[i];
          assertEquals("Seed " + seed, expected, actual);
        }
      }
    }
  }

  @Test
  public void testReadingWritingBufferLargerThanBlockSize() throws Exception {
    Random random = new Random(1);
    byte[] buf = new byte[122880];
    random.nextBytes(buf);

    File file = new File(storePathDir, "testReadingWritingBufferLargerThanBlockSize");
    try (FileBlockStore store = new FileBlockStore(file, 100000000, false)) {
      {
        long pos = 139264;
        int len = buf.length;
        int offset = 0;
        while (len > 0) {
          int write = store.write(pos, buf, offset, len);
          pos += write;
          offset += write;
          len -= write;
        }
      }
      byte[] buf2 = new byte[buf.length];
      {
        long pos = 139264;
        int len = buf.length;
        int offset = 0;
        while (len > 0) {
          int read = store.read(pos, buf2, offset, len);
          pos += read;
          offset += read;
          len -= read;
        }
      }

      assertTrue(Arrays.equals(buf, buf2));
    }
  }

  private int getLength(long pos, int length, int blockSize) {
    int blockOffset = (int) (pos % blockSize);
    int len = blockSize - blockOffset;
    return Math.min(len, length);
  }
}
