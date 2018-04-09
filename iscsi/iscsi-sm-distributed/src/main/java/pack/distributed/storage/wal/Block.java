package pack.distributed.storage.wal;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@Builder
@EqualsAndHashCode
public class Block {

  int blockId;
  long transId;
  byte[] data;

  public int getMemorySize() {
    return data.length + 16;
  }

}
