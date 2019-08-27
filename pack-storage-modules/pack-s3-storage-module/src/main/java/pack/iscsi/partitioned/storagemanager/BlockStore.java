package pack.iscsi.partitioned.storagemanager;

import java.io.IOException;
import java.util.List;

public interface BlockStore {

  void updateGeneration(long volumeId, long blockId, long generation) throws IOException;

  long getGeneration(long volumeId, long blockId) throws IOException;

  long getVolumeId(String name);

  int getBlockSize(long volumeId);

  long getLengthInBytes(long volumeId);

  List<String> getVolumeNames();

}
