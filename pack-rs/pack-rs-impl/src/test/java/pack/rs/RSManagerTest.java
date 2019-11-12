package pack.rs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSManagerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSManagerTest.class);

  @Test
  public void testRSManager() throws IOException {
    long seed = getRandomSeed();

    Random random = new Random(seed);
    int minStripeSize = 1024;// getInt(random, 10, 10000);// 1024;
    int parts = getInt(random, 1, 500);// 512;
    int dataPartCount = 4; // 4;
    int parityPartCount = getInt(random, 1, 4);

    LOGGER.info("seed {} minStripeSize {} parts {} dataPartCount {} parityPartCount {}", seed, minStripeSize, parts,
        dataPartCount, parityPartCount);

    RSProcessor manager = RSManager.create(parts * minStripeSize, dataPartCount, parityPartCount, minStripeSize);

    ByteBuffer input = ByteBuffer.allocate(parts * minStripeSize);
    byte[] buf = new byte[minStripeSize];
    for (int i = 0; i < parts; i++) {
      random.nextBytes(buf);
      input.put(buf);
    }
    input.flip();

    manager.write(input);
    ByteBuffer[] outputParts = manager.finish();

    ByteBuffer output = ByteBuffer.allocate(parts * minStripeSize);
    outputParts[2] = null; // missing data part
    manager.read(outputParts, output);

    RSManager.reset(input, output);
    assertEquals("Seed=" + seed, input, output);
  }

  private int getInt(Random random, int min, int max) {
    return random.nextInt(max - min) + min;
  }

  private long getRandomSeed() {
    return System.nanoTime();
  }

}
