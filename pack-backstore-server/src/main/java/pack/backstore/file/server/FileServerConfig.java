package pack.backstore.file.server;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.backstore.config.ServerConfig;

@Value
@Builder(toBuilder = true)
public class FileServerConfig implements ServerConfig {
  
  public static int DEFAULT_PORT = 8362;

  File storeDir;

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

  @Builder.Default
  int maxFileHandles = 1000;

  @Builder.Default
  LockIdManager lockIdManager = LockIdManager.NO_LOCKS;

}
