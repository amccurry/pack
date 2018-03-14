package pack.distributed.storage.status;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class UpdateBlockId {
  int blockId;
  long transId;
  String volume;
}
