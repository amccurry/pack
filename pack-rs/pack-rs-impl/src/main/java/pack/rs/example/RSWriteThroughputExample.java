package pack.rs.example;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import pack.rs.RSManager;
import pack.rs.RSProcessor;

public class RSWriteThroughputExample {

  public static void main(String[] args) throws IOException {
    int seed = 1;
    int minStripeSize = 1024;
    int dataPartCount = 4;
    int parityPartCount = 2;

    RSProcessor manager = RSManager.create(1024 * 1024, dataPartCount, parityPartCount, minStripeSize);
    Random random = new Random(seed);

    ByteBuffer input = ByteBuffer.allocate(524288);
    byte[] buf = new byte[minStripeSize];
    int parts = 512;
    for (int i = 0; i < parts; i++) {
      random.nextBytes(buf);
      input.put(buf);
    }
    input.flip();

    long total = 0;
    long start = System.nanoTime();
    while (true) {
      manager.reset();
      long now = System.nanoTime();
      if (start + 5_000_000_000L < now) {
        double seconds = (now - start) / 1_000_000_000.0;
        double mibsPerSecond = ((double) total) / 1024.0 / 1024.0 / seconds;
        System.out.println(total + " " + mibsPerSecond + " MiB/s");
        start = System.nanoTime();
        total = 0;
      }

      RSManager.reset(input);
      total += input.limit();
      manager.write(input);
      manager.finish();
    }
  }

}