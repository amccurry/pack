package pack.distributed.storage.status;

import java.util.List;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BlockUpdateInfoBatch {
  List<BlockUpdateInfo> batch;
  String volume;
}
