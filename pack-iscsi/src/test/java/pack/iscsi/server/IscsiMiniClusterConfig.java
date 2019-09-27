package pack.iscsi.server;

import java.io.File;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class IscsiMiniClusterConfig {

  @Builder.Default
  Set<String> addresses = ImmutableSet.of("127.0.0.1");

  @Builder.Default
  int port = 0;

  File storageDir;

  long maxCacheSizeInBytes = 1_000_000_000;

  int walServerCount = 3;

}
