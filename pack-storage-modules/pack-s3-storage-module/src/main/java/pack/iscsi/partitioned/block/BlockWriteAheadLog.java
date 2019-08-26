package pack.iscsi.partitioned.block;

import java.io.IOException;

public interface BlockWriteAheadLog {

  void write(long volumeId, long blockId, long generation, byte[] bytes, int offset, int len) throws IOException;

}
