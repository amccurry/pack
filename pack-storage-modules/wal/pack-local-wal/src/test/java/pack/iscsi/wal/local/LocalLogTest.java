package pack.iscsi.wal.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import pack.iscsi.io.FileIO;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.wal.local.LocalWriteAheadLogger;

public class LocalLogTest {

  private static final File DIR = new File("./target/tmp/LocalLogTest");
  private static final File DIR_TMP = new File("./target/tmp/LocalLogTestTmp");

  @Before
  public void setup() {
    IOUtils.rmr(DIR);
    DIR.mkdirs();
    IOUtils.rmr(DIR_TMP);
    DIR_TMP.mkdirs();
  }

  @Test
  public void testLocalLog() throws IOException {
    long seed = 1;

    int length = 10_000_000;
    int bufferSize = 100;
    int passes = 1;

    File actual = new File(DIR_TMP, UUID.randomUUID()
                                        .toString());

    File recover = new File(DIR_TMP, UUID.randomUUID()
                                         .toString());

    try (RandomAccessFile raf = new RandomAccessFile(actual, "rw")) {
      raf.setLength(length);
      Random random = new Random(seed);
      FileChannel channel = raf.getChannel();
      byte[] buffer = new byte[bufferSize];
      try (LocalWriteAheadLogger log = new LocalWriteAheadLogger(DIR, 0, 0)) {
        long generation = 1;
        for (int i = 0; i < passes; i++) {
          int position = random.nextInt(length - bufferSize);
          random.nextBytes(buffer);
          log.append(generation++, position, buffer, 0, buffer.length);
          ByteBuffer bb = ByteBuffer.wrap(buffer, 0, buffer.length);
          while (bb.remaining() > 0) {
            position += channel.write(bb, position);
          }
        }
      }
    }

    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(recover, 4096, "rw")) {
      randomAccessIO.setLength(length);
      try (LocalWriteAheadLogger log = new LocalWriteAheadLogger(DIR, 0, 0)) {
        long generation = 0;
        long recoveredGeneration = log.recover(randomAccessIO, generation);
        assertEquals(passes, recoveredGeneration);
      }
    }

    try (RandomAccessFile actualRaf = new RandomAccessFile(actual, "r");
        RandomAccessFile recoverRaf = new RandomAccessFile(recover, "r")) {

      byte[] actualBuffer = new byte[bufferSize];
      byte[] recoverBuffer = new byte[bufferSize];

      long len = length;

      int i = 0;
      while (len > 0) {
        int l = (int) Math.min(len, bufferSize);
        actualRaf.readFully(actualBuffer, 0, l);
        recoverRaf.readFully(recoverBuffer, 0, l);
        assertTrue("pass " + i++, Arrays.equals(actualBuffer, recoverBuffer));
        len -= l;
      }
    }

  }

}
