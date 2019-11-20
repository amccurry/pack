package pack.volume.server;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class VolumeServerConfig {

  @Builder.Default
  String address = "0.0.0.0";

  @Builder.Default
  int port = 0;

  CuratorFramework curatorFramework;

  String zkPrefix;

  @Builder.Default
  int clientTimeout = (int) TimeUnit.SECONDS.toMillis(1000);

  @Builder.Default
  int minThreads = 10;

  @Builder.Default
  int maxThreads = 10;

}
