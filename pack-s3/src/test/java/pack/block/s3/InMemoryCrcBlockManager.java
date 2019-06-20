package pack.block.s3;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import pack.block.CrcBlockManager;
import pack.block.util.CRC64;

public class InMemoryCrcBlockManager {

  public static CrcBlockManager newInstance() {

    ConcurrentMap<Long, Long> crcs = new ConcurrentHashMap<>();

    return new CrcBlockManager() {

      @Override
      public void sync() {

      }

      @Override
      public void putBlockCrc(long blockId, long crc) {
        crcs.put(blockId, crc);
      }

      @Override
      public long getBlockCrc(long blockId) {
        Long crc = crcs.get(blockId);
        if (crc == null) {
          return CRC64.DEFAULT_VALUE;
        }
        return crc;
      }

      @Override
      public void close() {

      }
    };
  }

}
