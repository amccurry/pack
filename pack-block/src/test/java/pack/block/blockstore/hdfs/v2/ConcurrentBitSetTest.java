package pack.block.blockstore.hdfs.v2;

import static org.junit.Assert.assertEquals;

import java.util.BitSet;
import java.util.Random;

import org.junit.Test;

public class ConcurrentBitSetTest {

  private static final int MAX_BITSET_LENGTH = 100000;

  @Test
  public void testConcurrentBitSet() {
    Random random = new Random();
    int length = random.nextInt(MAX_BITSET_LENGTH);
    System.out.println(length);
    ConcurrentBitSet concurrentBitSet = new ConcurrentBitSet(length);
    BitSet bitSet = new BitSet(length);
    for (int i = 0; i < 1000; i++) {
      int index = random.nextInt(length);
      concurrentBitSet.set(index);
      bitSet.set(index);
    }

    for (int i = 0; i < length; i++) {
      assertEquals(bitSet.get(i), concurrentBitSet.get(i));
    }
  }

}
