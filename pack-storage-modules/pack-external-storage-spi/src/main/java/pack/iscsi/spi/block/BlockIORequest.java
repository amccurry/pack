package pack.iscsi.spi.block;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.RandomAccessIO;

@Value
@Builder
public class BlockIORequest {
  int blockSize;
  long volumeId;
  long blockId;
  long onDiskGeneration;
  BlockState onDiskState;
  long lastStoredGeneration;
  RandomAccessIO randomAccessIO;
}
