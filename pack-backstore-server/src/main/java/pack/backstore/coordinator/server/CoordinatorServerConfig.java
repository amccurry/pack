package pack.backstore.coordinator.server;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class CoordinatorServerConfig {

  public static String ZK_PREFIX_DEFAULT = "/pack/backstore";

  @Builder.Default
  String hostname = "0.0.0.0";

  @Builder.Default
  int port = 8363;

  @Builder.Default
  int clientTimeout = 10000;

  @Builder.Default
  int minThreads = 10;

  @Builder.Default
  int maxThreads = 100;

  @Builder.Default
  int maxFileHandles = 1000;

  String zkConnection;

  @Builder.Default
  String zkPrefix = ZK_PREFIX_DEFAULT;

}
