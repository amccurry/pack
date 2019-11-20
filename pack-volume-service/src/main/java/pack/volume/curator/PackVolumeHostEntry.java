package pack.volume.curator;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@EqualsAndHashCode
public class PackVolumeHostEntry {
  String hostname;
  int port;
}