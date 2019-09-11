package pack.iscsi.spi.block;

import java.nio.channels.FileChannel;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BlockIORequest {
  FileChannel channel;
  int blockSize;
  long volumeId;
  long blockId;
  long onDiskGeneration;
  BlockState onDiskState;
  long lastStoredGeneration;
}
