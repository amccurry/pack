package pack.block.server.json;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class BlockPackFuseConfig {
  String volumeName;
  String fuseMountLocation;
  String fsMountLocation;
  String fsMetricsLocation;
  String fsLocalCache;
  String hdfsVolumePath;
  String zkConnection;
  int zkTimeout;
  String unixSock;
  int numberOfMountSnapshots;
  long volumeMissingPollingPeriod;
  int volumeMissingCountBeforeAutoShutdown;
  boolean countDockerDownAsMissing;
  boolean fileSystemMount;
}
