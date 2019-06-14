package pack.s3;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CrcManager {

  private final Map<Long, Long> _crcs = new ConcurrentHashMap<>();

  public void setCrc(long blockId, long crc) {
    _crcs.put(blockId, crc);
  }

  public long getCrc(long blockId) {
    Long value = _crcs.get(blockId);
    if (value == null) {
      return -1L;
    }
    return value;
  }

}
