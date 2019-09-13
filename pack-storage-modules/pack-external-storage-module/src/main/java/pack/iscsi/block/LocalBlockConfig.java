package pack.iscsi.block;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.volume.BlockGenerationStore;
import pack.iscsi.volume.VolumeMetadata;

@Value
@Builder
public class LocalBlockConfig {

  File blockDataDir;

  VolumeMetadata volumeMetadata;

  long blockId;

  BlockGenerationStore blockStore;

  BlockWriteAheadLog wal;

  @Builder.Default
  long syncTimeAfterIdle = 30;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.SECONDS;
}
