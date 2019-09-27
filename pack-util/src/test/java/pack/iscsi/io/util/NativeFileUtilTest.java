package pack.iscsi.io.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.smacke.jaydio.DirectRandomAccessFile;
import pack.iscsi.io.IOUtils;
import pack.iscsi.io.util.NativeFileUtil.FallocateMode;

public class NativeFileUtilTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeFileUtilTest.class);
  private static final File DIR = new File("./target/tmp/NativeFileUtilTest");

  @Before
  public void setup() {
    IOUtils.rmr(DIR);
    DIR.mkdirs();
  }

  @Test
  public void testFallocate() throws Exception {
    File file = new File(DIR, UUID.randomUUID()
                                  .toString());

    int numberOfBlocksToPunch = 64;

    long length = 1024 * 1024;
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      raf.write(new byte[(int) length]);
    }

    try (DirectRandomAccessFile draf = new DirectRandomAccessFile(file, "rw", 8192)) {
      long blockCountBefore = BlockCount.getBlockCount(file);
      /*
       * Even though the FS will report a block size of 512 the actual block
       * size is dictated by the FS implementation and xfs seems to have an
       * actual block size of 4K.
       */
      int blockSize = (int) (length / blockCountBefore);
      long offset = blockSize * 8;
      long len = blockSize * numberOfBlocksToPunch;

      LOGGER.info("length {} bc before {} block size {} offset {} punch length {}", length, blockCountBefore, blockSize,
          offset, len);

      NativeFileUtil.fallocate(draf, offset, len, FallocateMode.FALLOC_FL_PUNCH_HOLE,
          FallocateMode.FALLOC_FL_KEEP_SIZE);

      long blockCountAfter = BlockCount.getBlockCount(file);
      assertEquals(blockCountBefore - numberOfBlocksToPunch, blockCountAfter);
      assertEquals(length, draf.length());
    }
    assertEquals(length, file.length());
  }

  @Test
  public void testFtruncate() throws Exception {
    File file = new File(DIR, UUID.randomUUID()
                                  .toString());

    long length = 1024 * 1024;
    NativeFileUtil.ftruncate(file, length);
    long blockCountAfter = BlockCount.getBlockCount(file);
    assertEquals(0, blockCountAfter);
    assertEquals(length, file.length());
  }

}
