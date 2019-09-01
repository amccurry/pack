package pack.iscsi.partitioned.storagemanager;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class VolumeMetadata {

  long volumeId;
  int blockSize;
  long lengthInBytes;

}
