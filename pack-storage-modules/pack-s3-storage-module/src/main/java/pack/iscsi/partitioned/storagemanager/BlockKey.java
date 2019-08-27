package pack.iscsi.partitioned.storagemanager;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BlockKey {

  long volumeId;
  long blockId;

}
