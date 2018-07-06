package pack.zk.utils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class ZkLockInfo {
  String path;
  long sesssionId;
}
