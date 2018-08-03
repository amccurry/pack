package pack.block.server.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class BlockPackFuseConfig {
  String volumeName;
  String fuseMountLocation;
  String fsMetricsLocation;
  String fsLocalCache;
  String hdfsVolumePath;
  int numberOfMountSnapshots;
  // long volumeMissingPollingPeriod;
  // int volumeMissingCountBeforeAutoShutdown;
  // boolean countDockerDownAsMissing;
}
