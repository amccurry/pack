package pack.iscsi.partitioned.storagemanager;

import java.io.IOException;
import java.util.List;

public interface VolumeStore {

  List<String> getVolumeNames();

  VolumeMetadata getVolumeMetadata(long volumeId) throws IOException;

  long getVolumeId(String name) throws IOException;

  default VolumeMetadata getVolumeMetadata(String name) throws IOException {
    long volumeId = getVolumeId(name);
    return getVolumeMetadata(volumeId);
  }

  void createVolume(String name, int blockSize, long lengthInBytes) throws IOException;

  void destroyVolume(long volumeId) throws IOException;

  void renameVolume(long volumeId, String name) throws IOException;

  void growVolume(long volumeId, long lengthInBytes) throws IOException;

  BlockStore getBlockStore(long volumeId) throws IOException;

}
