package pack.distributed.storage.kafka;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Block {

  int blockId;
  long transId;
  byte[] data;

}
