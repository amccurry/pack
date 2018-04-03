package pack.distributed.storage.zk;

import lombok.*;

@Value
@Builder
public class PackZooKeeperServerConfig {

  String hostname;
  long id;
  int clientPort;
  int peerPort;
  int leaderElectPort;

}
