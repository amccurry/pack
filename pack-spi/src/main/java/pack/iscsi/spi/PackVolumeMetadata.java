package pack.iscsi.spi;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder(toBuilder = true)
public class PackVolumeMetadata {

  String name;
  long volumeId;
  int blockSizeInBytes;
  long lengthInBytes;
  String assignedHostname;

}
