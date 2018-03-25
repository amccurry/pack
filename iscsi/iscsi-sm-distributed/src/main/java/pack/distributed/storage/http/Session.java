package pack.distributed.storage.http;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder
public class Session implements Comparable<Session> {

  String iqn;
  String targetServerAddress;
  String clientAddress;
  String writeLockClientAddress;

  @Override
  public int compareTo(Session o) {
    int compare = getIqn().compareTo(o.getIqn());
    if (compare == 0) {
      if (compare == 0) {
        compare = getWriteLockClientAddress().compareTo(o.getWriteLockClientAddress());
        if (compare == 0) {
          compare = getTargetServerAddress().compareTo(o.getTargetServerAddress());
          if (compare == 0) {
            compare = getClientAddress().compareTo(o.getClientAddress());
          }
        }
      }
    }
    return compare;
  }

}
