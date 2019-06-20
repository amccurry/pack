package pack.block;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class BlockConfig {

  long blockSize;
  long blockId;
  String volume;
  CrcBlockManager crcBlockManager;

}
