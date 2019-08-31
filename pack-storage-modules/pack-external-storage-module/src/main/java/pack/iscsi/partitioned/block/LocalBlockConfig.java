package pack.iscsi.partitioned.block;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;

@Value
@Builder
public class LocalBlockConfig {

  File blockDataDir;

  long volumeId;

  long blockId;

  int blockSize;

  BlockStore blockStore;

  BlockWriteAheadLog wal;

  @Builder.Default
  long syncTimeAfterIdle = 1;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.MINUTES;
}
