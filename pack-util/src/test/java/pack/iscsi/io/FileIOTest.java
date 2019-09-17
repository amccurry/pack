package pack.iscsi.io;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import pack.iscsi.spi.RandomAccessIO;

public class FileIOTest {

  @Test
  public void testLength() throws IOException {
    File file = new File("./target/tmp/FileIOTest/" + UUID.randomUUID()
                                                          .toString());
    file.getParentFile()
        .mkdirs();
    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, 4096, "rw")) {
      randomAccessIO.write(1);
      assertEquals(1, randomAccessIO.length());
      randomAccessIO.setLength(1000);
      randomAccessIO.seek(0);
      byte[] buf = new byte[1000];
      randomAccessIO.readFully(buf);
      assertEquals((byte) 1, buf[0]);
      assertEquals(1000, randomAccessIO.length());
    }

    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, 4096, "rw")) {
      assertEquals(1000, randomAccessIO.length());
    }
    file.delete();
  }

}
