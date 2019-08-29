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

}
