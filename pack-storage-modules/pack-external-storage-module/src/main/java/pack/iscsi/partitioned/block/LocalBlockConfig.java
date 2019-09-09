package pack.iscsi.partitioned.block;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.partitioned.storagemanager.BlockGenerationStore;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
import pack.iscsi.partitioned.storagemanager.VolumeMetadata;

@Value
@Builder
public class LocalBlockConfig {

  File blockDataDir;

  VolumeMetadata volumeMetadata;

  long blockId;

  BlockGenerationStore blockStore;

  BlockWriteAheadLog wal;

  @Builder.Default
  long syncTimeAfterIdle = 1;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.MINUTES;
}
