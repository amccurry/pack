package pack.iscsi.volume;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.block.BlockCacheMetadataStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

@Value
@Builder
public class BlockStorageModuleConfig {

  MetricsFactory metricsFactory;

  BlockGenerationStore blockGenerationStore;

  BlockWriteAheadLog writeAheadLog;

  BlockStateStore blockStateStore;

  BlockCacheMetadataStore blockCacheMetadataStore;

  BlockIOFactory externalBlockStoreFactory;

  String volumeName;

  long volumeId;

  int blockSize;

  long blockCount;

  boolean readOnly;

  File blockDataDir;

  long maxCacheSizeInBytes;

  @Builder.Default
  long syncTimeAfterIdle = 5;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.SECONDS;

  @Builder.Default
  int syncExecutorThreadCount = 5;

  @Builder.Default
  int cachePreloadExecutorThreadCount = 5;

  @Builder.Default
  int readAheadExecutorThreadCount = 20;

  @Builder.Default
  int readAheadBlockLimit = 20;

}