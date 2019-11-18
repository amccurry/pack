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

@Value
@Builder
public class BlockStorageModuleConfig {

  MetricsFactory metricsFactory;

  BlockGenerationStore blockGenerationStore;

  BlockStateStore blockStateStore;

  BlockCacheMetadataStore blockCacheMetadataStore;

  BlockIOFactory externalBlockStoreFactory;

  String volumeName;

  long volumeId;

  int blockSize;

  long blockCount;

  boolean readOnly;

  File[] blockDataDirs;

  long maxCacheSizeInBytes;

  @Builder.Default
  long syncTimeAfterIdle = 5;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.SECONDS;

  @Builder.Default
  int syncExecutorThreadCount = 20;

  @Builder.Default
  int readAheadBlockLimit = 20;

  @Builder.Default
  int readAheadExecutorThreadCount = 20;

}