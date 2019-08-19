package pack.iscsi.server;

import java.io.File;
import java.util.Set;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.manager.TargetManager;
import pack.iscsi.spi.StorageModuleFactory;

@Value
@Builder
public class IscsiServerConfig {

  Set<String> addresses;
  int port;
  TargetManager iscsiTargetManager;
  File cacheDir;
  StorageModuleFactory iStorageModuleFactory;

}
