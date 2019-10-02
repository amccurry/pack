package pack.iscsi.spi;

import java.io.IOException;
import java.util.Map;

public interface VolumeListener {

  default void lengthChange(PackVolumeMetadata packVolumeMetadata) throws IOException {
    throw new IOException("Does not implement length change");
  }

  default void sync(PackVolumeMetadata packVolumeMetadata, boolean blocking, boolean onlyIfIdleWrites)
      throws IOException {
    throw new IOException("Does not implement sync");
  }

  default Map<BlockKey, Long> createSnapshot(PackVolumeMetadata packVolumeMetadata) throws IOException {
    throw new IOException("Does not implement create snapshot");
  }

  default boolean hasVolume(PackVolumeMetadata metadata) throws IOException {
    throw new IOException("Does not implement has volume");
  }

}
