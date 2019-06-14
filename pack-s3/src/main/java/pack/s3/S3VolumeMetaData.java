package pack.s3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.Writable;

public class S3VolumeMetaData implements Writable {

  private static final long DEFAULT_MISSING_CRC = -1L;
  private final Map<Long, Long> _crcs = new ConcurrentHashMap<>();

  public Map<Long, Long> getCrcs() {
    return _crcs;
  }

  public void setCrc(long blockId, long sync) {
    _crcs.put(blockId, sync);
  }

  public long getCrc(long blockId) {
    Long value = _crcs.get(blockId);
    if (value == null) {
      return DEFAULT_MISSING_CRC;
    }
    return value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(_crcs.size());
    for (Entry<Long, Long> e : _crcs.entrySet()) {
      out.writeLong(e.getKey());
      out.writeLong(e.getValue());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      long key = in.readLong();
      long value = in.readLong();
      _crcs.put(key, value);
    }
  }

}
