package pack.iscsi.volume;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.block.BlockCacheMetadataStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

@Value
@Builder(toBuilder = true)
public class BlockStorageModuleFactoryConfig {

  PackVolumeStore packVolumeStore;

  BlockGenerationStore blockStore;

  BlockWriteAheadLog writeAheadLog;

  BlockIOFactory externalBlockStoreFactory;

  BlockStateStore blockStateStore;

  BlockCacheMetadataStore blockCacheMetadataStore;

  File blockDataDir;

  long maxCacheSizeInBytes;

  @Builder.Default
  MetricsFactory metricsFactory = MetricsFactory.NO_OP;

  @Builder.Default
  long gcDriverTime = 1;

  @Builder.Default
  TimeUnit gcDriverTimeUnit = TimeUnit.MINUTES;

  @Builder.Default
  int gcExecutorThreadCount = 10;

  @Builder.Default
  long defaultSyncTimeAfterIdle = 5;

  @Builder.Default
  TimeUnit defaultSyncTimeAfterIdleTimeUnit = TimeUnit.SECONDS;

  @Builder.Default
  int defaultSyncExecutorThreadCount = 20;

  @Builder.Default
  int defaultReadAheadExecutorThreadCount = 20;

  @Builder.Default
  int defaultReadAheadBlockLimit = 20;

}
