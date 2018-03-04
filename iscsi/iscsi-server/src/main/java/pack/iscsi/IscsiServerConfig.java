package pack.iscsi;

import java.util.Set;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.storage.StorageTargetManager;

@Value
@Builder
public class IscsiServerConfig {

  Set<String> addresses;
  int port;
  StorageTargetManager iscsiTargetManager;
  String serialId;

}
