package pack.iscsi.io;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.UUID;

import org.junit.Test;

import net.smacke.jaydio.DirectRandomAccessFile;
import pack.iscsi.spi.RandomAccessIO;

public class FileIOTest {

  @Test
  public void testLength() throws IOException {
    File file = new File("./target/tmp/FileIOTest/" + UUID.randomUUID()
                                                          .toString());
    file.getParentFile()
        .mkdirs();
    int blockSize = 4096;

    FileIO.setDirectIOEnabled(false);

    FileIO.setLengthFile(file, 1);
    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, blockSize, "rw")) {
      randomAccessIO.write(new byte[] { 1 });
      assertEquals(1, randomAccessIO.length());
    }

    FileIO.setLengthFile(file, 1000);
    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, blockSize, "rw")) {
      randomAccessIO.seek(0);
      byte[] buf = new byte[1000];
      randomAccessIO.readFully(buf);
      assertEquals((byte) 1, buf[0]);
      assertEquals(1000, randomAccessIO.length());
    }

    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, blockSize, "rw")) {
      assertEquals(1000, randomAccessIO.length());
    }
    file.delete();
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
