package pack.iscsi.brick.remote.curator;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@EqualsAndHashCode
public class PackBrickHostEntry {
  String hostname;
  int port;
}