package pack.block.zk;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class ZkCrcBlockManagerConfig {

  String zk;
  String volume;

}
