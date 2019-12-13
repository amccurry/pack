package pack.backstore.file.server;

import java.io.File;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class FileServerConfig {

  File storeDir;

  @Builder.Default
  String hostname = "0.0.0.0";

  @Builder.Default
  int port = 8362;

  @Builder.Default
  int clientTimeout = 10000;

  @Builder.Default
  int minThreads = 10;

  @Builder.Default
  int maxThreads = 100;

  @Builder.Default
  int maxFileHandles = 1000;

  @Builder.Default
  LockIdManager lockIdManager = LockIdManager.NO_LOCKS;

}
