package pack.iscsi.volume;

import java.io.File;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.MetricsFactory;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

@Value
@Builder(toBuilder = true)
public class BlockStorageModuleFactoryConfig {

  PackVolumeStore packVolumeStore;

  BlockGenerationStore blockStore;

  BlockWriteAheadLog writeAheadLog;

  BlockIOFactory externalBlockStoreFactory;

  File blockDataDir;

  long maxCacheSizeInBytes;

  @Builder.Default
  long syncTimeAfterIdle = 1;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.MINUTES;

  @Builder.Default
  int syncThreads = ForkJoinPool.getCommonPoolParallelism();

  @Builder.Default
  MetricsFactory metricsFactory = MetricsFactory.NO_OP;
}
