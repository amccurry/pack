package pack.block.blockstore.crc;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrcLayer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CrcLayer.class);

  public static void main(String[] args) {

    CrcLayer layer = new CrcLayer(100, 4096);
    Random random = new Random(1);
    byte[] buf = new byte[128];
    for (int i = 0; i < 100; i++) {
      random.nextBytes(buf);
      layer.put(i, buf);
    }
    random = new Random(1);
    for (int i = 0; i < 100; i++) {
      random.nextBytes(buf);
      layer.validate(i, buf);
    }
  }

  private final int[] _crcs;
  private final BitSet _presentEntries;

  public CrcLayer(int entries, int blockSize) {
    _presentEntries = new BitSet(entries);
    _crcs = new int[entries];
    CRC32 crc32 = new CRC32();
    crc32.update(new byte[blockSize]);
    int emtpyValue = (int) crc32.getValue();
    System.out.println(emtpyValue);
    Arrays.fill(_crcs, emtpyValue);
  }

  public synchronized void put(int blockId, byte[] buf) {
    CRC32 crc32 = new CRC32();
    crc32.update(buf);
    _crcs[blockId] = (int) crc32.getValue();
    _presentEntries.set(blockId);
  }

  public synchronized void validate(int blockId, byte[] buf) {
    validate(blockId, buf, 0, buf.length);
  }

  public synchronized void validate(int blockId, byte[] buf, int off, int len) {
    if (_presentEntries.get(blockId)) {
      CRC32 crc32 = new CRC32();
      crc32.update(buf, off, len);
      int validCrc = _crcs[blockId];
      int dataCrc = (int) crc32.getValue();
      if (validCrc != dataCrc) {
        LOGGER.error("CRC error on block {} expected {} actual {}", blockId, validCrc, dataCrc);
      }
    }
  }

}
