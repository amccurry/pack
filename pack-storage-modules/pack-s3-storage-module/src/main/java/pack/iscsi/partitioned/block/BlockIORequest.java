package pack.iscsi.partitioned.block;

import java.io.File;
import java.nio.channels.FileChannel;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BlockIORequest {
  FileChannel channel;
  File fileForReadingOnly; // @TODO Remove this soon
  int blockSize;
  long volumeId;
  long blockId;
  long onDiskGeneration;
  BlockState onDiskState;
  long lastStoredGeneration;
}
