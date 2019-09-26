package pack.iscsi.volume;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

@Value
@Builder
public class BlockStorageModuleConfig {

  String volumeName;

  long volumeId;

  int blockSize;

  long lengthInBytes;

  BlockIOFactory externalBlockStoreFactory;

  @Builder.Default
  long syncTimeAfterIdle = 1;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.MINUTES;

  ExecutorService syncExecutor;

  MetricsFactory metricsFactory;

  BlockGenerationStore blockGenerationStore;

  BlockWriteAheadLog writeAheadLog;

  File blockDataDir;

  long maxCacheSizeInBytes;

}