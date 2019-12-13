package pack.backstore.coordinator.server;

import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.backstore.config.ServerConfig;

@Value
@Builder(toBuilder = true)
public class CoordinatorServerConfig implements ServerConfig {

  public static String ZK_PREFIX_DEFAULT = "/pack/backstore";
  
  public static int DEFAULT_PORT = 8363;

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

  String zkConnection;

  @Builder.Default
  String zkPrefix = ZK_PREFIX_DEFAULT;

}
