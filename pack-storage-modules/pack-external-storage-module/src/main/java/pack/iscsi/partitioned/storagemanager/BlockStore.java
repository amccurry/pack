package pack.iscsi.partitioned.storagemanager;

import java.io.IOException;
import java.util.List;

public interface BlockStore {

  List<String> getVolumeNames();

  long getVolumeId(String name);

  int getBlockSize(long volumeId);

  long getLengthInBytes(long volumeId);

  long getLastStoreGeneration(long volumeId, long blockId) throws IOException;

  void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException;

  long createVolume(String name, int blockSize, long lengthInBytes) throws IOException;

  void destroyVolume(long volumeId) throws IOException;

  void renameVolume(long volumeId, String name) throws IOException;

  void growVolume(long volumeId, long lengthInBytes) throws IOException;

}
