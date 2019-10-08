package pack.iscsi.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;

import net.smacke.jaydio.DirectRandomAccessFile;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.RandomAccessIOReader;

public class FileIOTest {

  @Test
  public void testLength() throws IOException {
    File file = new File("./target/tmp/FileIOTest/" + UUID.randomUUID()
                                                          .toString());
    file.getParentFile()
        .mkdirs();
    int blockSize = 4096;

    FileIO.setDirectIOEnabled(true);

    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, blockSize, "rw")) {
      randomAccessIO.setLength(1);
      randomAccessIO.write(0, new byte[] { 1 });
      assertEquals(1, randomAccessIO.length());
    }

    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, blockSize, "rw")) {
      randomAccessIO.setLength(1000);
      byte[] buf = new byte[1000];
      randomAccessIO.read(0, buf);
      assertEquals((byte) 1, buf[0]);
      assertEquals(1000, randomAccessIO.length());
    }

    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, blockSize, "rw")) {
      assertEquals(1000, randomAccessIO.length());
    }
    file.delete();
  }

  @Test
  public void testReadOnlyClone() throws IOException {
    File file = new File("./target/tmp/FileIOTest/" + UUID.randomUUID()
                                                          .toString());
    file.getParentFile()
        .mkdirs();
    int blockSize = 4096;

    Random random = new Random();
    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, blockSize, "rw")) {
      byte[] buffer1 = new byte[blockSize];
      random.nextBytes(buffer1);
      randomAccessIO.write(1000, buffer1);
      try (RandomAccessIOReader reader = randomAccessIO.cloneReadOnly()) {
        byte[] buffer2 = new byte[blockSize];
        reader.read(1000, buffer2);
        assertTrue(Arrays.equals(buffer1, buffer2));
      }
      assertEquals(1000 + buffer1.length, randomAccessIO.length());
    }
  }

  @Test
  public void testLengthDirect() throws IOException {
    File file = new File("./target/tmp/FileIOTest/" + UUID.randomUUID()
                                                          .toString());
    file.getParentFile()
        .mkdirs();
    try (DirectRandomAccessFile draf = new DirectRandomAccessFile(file, "rw")) {
      draf.write(new byte[] { 1 });
      assertEquals(1, draf.length());
    }

    assertEquals(1, file.length());

    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      raf.setLength(1000);
    }

    assertEquals(1000, file.length());

    try (DirectRandomAccessFile draf = new DirectRandomAccessFile(file, "rw")) {
      draf.seek(0);
      byte[] buf = new byte[1000];
      draf.readFully(buf);
      assertEquals((byte) 1, buf[0]);
      assertEquals(1000, draf.length());
    }

    try (DirectRandomAccessFile draf = new DirectRandomAccessFile(file, "rw")) {
      assertEquals(1000, draf.length());
    }

    file.delete();
  }

}
