package pack.block.s3;

import static org.junit.Assert.assertTrue;

import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class TestBlockDev {

  @Test
  public void test1() throws Exception {

    String file = "testmnt/testv2";

    int passes = 2;
    for (int pass = 0; pass < passes; pass++) {

      long seed = 1;
      long max = 64 * 1024 * 1024 * 10L;
      int bufferSize = 128 * 1000;

      try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        Random random = new Random(seed);
        byte[] buffer = new byte[bufferSize];
        for (long l = 0; l < max; l += buffer.length) {
          random.nextBytes(buffer);
          raf.write(buffer);
        }
      }

      try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        Random random = new Random(seed);
        byte[] assertBuffer = new byte[bufferSize];

        byte[] buffer = new byte[bufferSize];
        for (long l = 0; l < max; l += buffer.length) {
          random.nextBytes(assertBuffer);
          raf.readFully(buffer);
          assertTrue(Arrays.equals(assertBuffer, buffer));
        }
      }
    }
  }

  @Test
  public void test2() throws Exception {

    String file = "testmnt/testv2";

    int passes = 2;
    for (int pass = 0; pass < passes; pass++) {

      long seed = pass;
      long max = 64 * 1024 * 1024 * 3L;
      int bufferSize = 128 * 1000;

      try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        Random random = new Random(seed);
        byte[] buffer = new byte[bufferSize];
        for (long l = 0; l < max; l += buffer.length) {
          random.nextBytes(buffer);
          raf.write(buffer);
        }
      }

      try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        Random random = new Random(seed);
        byte[] assertBuffer = new byte[bufferSize];

        byte[] buffer = new byte[bufferSize];
        for (long l = 0; l < max; l += buffer.length) {
          random.nextBytes(assertBuffer);
          raf.readFully(buffer);
          assertTrue(Arrays.equals(assertBuffer, buffer));
        }
      }
    }
  }

}
