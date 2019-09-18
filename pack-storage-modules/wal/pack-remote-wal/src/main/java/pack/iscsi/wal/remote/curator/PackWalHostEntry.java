package pack.iscsi.wal.remote.curator;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@EqualsAndHashCode
public class PackWalHostEntry {
  String hostname;
  int port;
}