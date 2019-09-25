package pack.iscsi.spi;

import java.io.IOException;

public interface VolumeLengthListener {
  
  void lengthChange(PackVolumeMetadata packVolumeMetadata) throws IOException;
  
}
