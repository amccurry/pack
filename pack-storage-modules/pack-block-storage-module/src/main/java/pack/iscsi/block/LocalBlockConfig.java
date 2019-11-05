package pack.iscsi.block;

import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

@Value
@Builder(toBuilder = true)
public class LocalBlockConfig {

  long volumeId;

  long blockId;

  int blockSize;

  RandomAccessIO randomAccessIO;

  BlockStateStore blockStateStore;

  BlockGenerationStore blockGenerationStore;

  BlockWriteAheadLog wal;

  @Builder.Default
  long syncTimeAfterIdle = 5;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.SECONDS;

}
