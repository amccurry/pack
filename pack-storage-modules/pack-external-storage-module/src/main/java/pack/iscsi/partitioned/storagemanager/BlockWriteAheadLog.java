package pack.iscsi.partitioned.storagemanager;

import java.io.IOException;

import pack.iscsi.partitioned.block.BlockIOExecutor;

public interface BlockWriteAheadLog {

  void write(long volumeId, long blockId, long generation, long position, byte[] bytes, int offset, int len)
      throws IOException;

  void release(long volumeId, long blockId, long generation) throws IOException;

  BlockIOExecutor getWriteAheadLogReader();

}
