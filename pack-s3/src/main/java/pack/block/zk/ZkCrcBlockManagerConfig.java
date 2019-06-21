package pack.block.zk;

import org.apache.curator.framework.CuratorFramework;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class ZkCrcBlockManagerConfig {

  String volume;
  CuratorFramework client;

}
