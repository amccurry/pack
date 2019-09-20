package pack.iscsi.block;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

@Value
@Builder
public class LocalBlockConfig {

  File blockDataDir;

  PackVolumeMetadata volumeMetadata;

  long blockId;

  BlockGenerationStore blockStore;

  BlockWriteAheadLog wal;

  @Builder.Default
  long syncTimeAfterIdle = 30;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.SECONDS;

  @Builder.Default
  int bufferSize = 64 * 1024;
}
