package pack.iscsi.partitioned.storagemanager;

import java.util.List;

public interface BlockStore {

  long getVolumeId(String name);

  int getBlockSize(long volumeId);

  long getLengthInBytes(long volumeId);

  List<String> getVolumeNames();

  long getLastStoreGeneration(long volumeId, long blockId);

  void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration);

}
