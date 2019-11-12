package pack.rs.example;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import pack.rs.RSManager;
import pack.rs.RSProcessor;

public class RSExample {

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

    RSManager.reset(input);
    manager.write(input);
    ByteBuffer[] outputParts = manager.finish();

    ByteBuffer output = ByteBuffer.allocate(524288);

    outputParts[2] = null; // missing data part

    manager.read(outputParts, output);

    RSManager.reset(input);

    System.out.println(equals(input, output));

  }

  private static boolean equals(ByteBuffer src, ByteBuffer dst) {
    RSManager.reset(src, dst);
    return src.equals(dst);
  }

}
