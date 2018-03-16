package pack.distributed.storage.status;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BlockUpdateInfo {
  int blockId;
  long transId;
}
