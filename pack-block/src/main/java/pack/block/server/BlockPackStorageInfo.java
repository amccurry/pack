package pack.block.server;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class BlockPackStorageInfo {

  String name;
  long volumeSize;
  long hdfsSize;

  int numberOfSnapshots;

  long hdfsSizeWithSnapshots;
  int numberOfBlockFiles;

  int numberOfWalFiles;
  long maxWalSize;

}
