package pack.block;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class BlockManagerConfig {

  long blockSize;
  long cacheSize;
  long idleWriteTime;
  long crcBlockSize;
  String volume;
  BlockFactory blockFactory;
  CrcBlockManager crcBlockManager;

}
