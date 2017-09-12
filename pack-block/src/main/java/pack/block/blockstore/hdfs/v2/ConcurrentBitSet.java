package pack.block.blockstore.hdfs.v2;

import java.util.concurrent.atomic.AtomicLongArray;

public class ConcurrentBitSet {

  private final AtomicLongArray _bits;

  public ConcurrentBitSet(long numBits) {
    int length = bits2words(numBits);
    _bits = new AtomicLongArray(length);
  }

  public static int bits2words(long numBits) {
    return (int) (((numBits - 1) >>> 6) + 1);
  }

  public boolean get(int index) {
    int wordNum = index >> 6; // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit
    // check.
    long bitmask = 1L << index;
    return (_bits.get(wordNum) & bitmask) != 0;
  }

  public boolean set(int index) {
    int wordNum = index >> 6; // div 64
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    long word, oword;
    do {
      word = _bits.get(wordNum);
      // if set another thread stole the lock
      if ((word & bitmask) != 0) {
        return false;
      }
      oword = word;
      word |= bitmask;
    } while (!_bits.compareAndSet(wordNum, oword, word));
    return true;
  }

  public void clear(int index) {
    int wordNum = index >> 6;
    int bit = index & 0x03f;
    long bitmask = 1L << bit;
    long word, oword;
    do {
      word = _bits.get(wordNum);
      oword = word;
      word &= ~bitmask;
    } while (!_bits.compareAndSet(wordNum, oword, word));
  }
}