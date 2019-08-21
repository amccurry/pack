package pack.iscsi.server;

import java.util.Set;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.manager.TargetManager;

@Value
@Builder
public class IscsiServerConfig {

  Set<String> addresses;
  int port;
  TargetManager iscsiTargetManager;

}
