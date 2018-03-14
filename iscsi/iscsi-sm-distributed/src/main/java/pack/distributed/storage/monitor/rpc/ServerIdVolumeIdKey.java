package pack.distributed.storage.monitor.rpc;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@Builder
@EqualsAndHashCode
public class ServerIdVolumeIdKey {

  int serverId;
  int volumeId;

}
