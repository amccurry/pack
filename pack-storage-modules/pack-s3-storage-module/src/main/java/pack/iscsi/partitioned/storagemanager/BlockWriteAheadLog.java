package pack.iscsi.partitioned.storagemanager;

import java.io.IOException;

public interface BlockWriteAheadLog {

  void write(long volumeId, long blockId, long generation, byte[] bytes, int offset, int len) throws IOException;

}
