package pack.rs.example;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import pack.rs.RSManager;

public class RSReadThroughputExample {

  public static void main(String[] args) throws IOException {
    int seed = 1;
    int minStripeSize = 1024;
    int dataPartCount = 4;
    int parityPartCount = 2;

    RSManager manager = RSManager.create(1024 * 1024, dataPartCount, parityPartCount, minStripeSize);
    Random random = new Random(seed);

    ByteBuffer input = ByteBuffer.allocate(524288);
    byte[] buf = new byte[minStripeSize];
    int parts = 512;
    for (int i = 0; i < parts; i++) {
      random.nextBytes(buf);
      input.put(buf);
    }
    input.flip();

    RSManager.reset(input);
    manager.write(input);
    ByteBuffer[] outputParts = manager.finish();

    ByteBuffer output = ByteBuffer.allocate(524288);
    long total = 0;
    long start = System.nanoTime();
    while (true) {
      RSManager.reset(outputParts);
      long now = System.nanoTime();
      if (start + 5_000_000_000L < now) {
        double seconds = (now - start) / 1_000_000_000.0;
        double mibsPerSecond = ((double) total) / 1024.0 / 1024.0 / seconds;
        System.out.println(total + " " + mibsPerSecond + " MiB/s");
        start = System.nanoTime();
        total = 0;
      }
      outputParts[2] = null; // missing data part
      manager.read(outputParts, output);
      total += output.limit();
    }
  }

}
