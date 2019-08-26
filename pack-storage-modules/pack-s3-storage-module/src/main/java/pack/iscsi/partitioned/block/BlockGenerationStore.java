package pack.iscsi.partitioned.block;

import java.io.IOException;

public interface BlockGenerationStore {

  void updateGeneration(long volumeId, long blockId, long generation) throws IOException;
  
  long getGeneration(long volumeId, long blockId) throws IOException;

}
