package pack.iscsi.volume.cache;

import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

@Value
@Builder
public class BlockCacheLoaderConfig {

  long volumeId;

  int blockSize;

  LocalFileCacheFactory localFileCache;

  BlockStateStore blockStateStore;

  BlockGenerationStore blockGenerationStore;

  BlockWriteAheadLog writeAheadLog;

  BlockIOFactory externalBlockStoreFactory;

  @Builder.Default
  long syncTimeAfterIdle = 1;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.MINUTES;

  BlockRemovalListener removalListener;

}
