package pack.iscsi.io.direct;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class DirectIOThroughout {

  public static void main(String[] args) throws IOException {
    File file = new File("./testfile");
    file.delete();
    try (DirectIO io = new DirectIO(file)) {
      writeTest(io);
      readTest(io);
    }
  }

  private static void readTest(DirectIO io) throws IOException {
    byte[] buffer = new byte[4096 * 128];
    long testLength = 1024L * 1024L * 1024L;
    long length = testLength;
    long position = 0;
    long start = System.nanoTime();
    while (length > 0) {
      io.read(position, buffer);
      position += buffer.length;
      length -= buffer.length;
    }
    long end = System.nanoTime();
    System.out.println((end - start) / 1_000_000.0);
    long lengthInMB = testLength / 1024L / 1024L;
    double seconds = (end - start) / 1_000_000_000.0;
    double rate = lengthInMB / seconds;
    System.out.println("Rate " + rate + " MB/s");
  }

  private static void writeTest(DirectIO io) throws IOException {
    byte[] buffer = new byte[4096 * 128];
    Random random = new Random();
    random.nextBytes(buffer);

    long testLength = 1024L * 1024L * 1024L;
    long length = testLength;
    long position = 0;
    long start = System.nanoTime();
    while (length > 0) {
      io.write(position, buffer);
      position += buffer.length;
      length -= buffer.length;
    }
    long end = System.nanoTime();
    System.out.println((end - start) / 1_000_000.0);
    long lengthInMB = testLength / 1024L / 1024L;
    double seconds = (end - start) / 1_000_000_000.0;
    double rate = lengthInMB / seconds;
    System.out.println("Rate " + rate + " MB/s");
  }
}
