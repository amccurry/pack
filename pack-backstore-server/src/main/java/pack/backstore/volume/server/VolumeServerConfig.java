package pack.backstore.volume.server;

import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.backstore.config.ServerConfig;

@Value
@Builder(toBuilder = true)
public class VolumeServerConfig implements ServerConfig {

  public static int DEFAULT_PORT = 8364;

  @Builder.Default
  String hostname = "0.0.0.0";

  @Builder.Default
  int port = DEFAULT_PORT;

  @Builder.Default
  int clientTimeout = (int) TimeUnit.SECONDS.toMillis(10);

  @Builder.Default
  int minThreads = 10;

  @Builder.Default
  int maxThreads = 100;

}
