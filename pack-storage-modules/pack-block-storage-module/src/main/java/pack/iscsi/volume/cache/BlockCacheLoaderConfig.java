package pack.iscsi.volume.cache;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.volume.VolumeStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

@Value
@Builder
public class BlockCacheLoaderConfig {
  VolumeStore volumeStore;
  BlockGenerationStore blockStore;
  BlockWriteAheadLog writeAheadLog;
  File blockDataDir;
  BlockIOFactory externalBlockStoreFactory;
  long syncTimeAfterIdle;
  TimeUnit syncTimeAfterIdleTimeUnit;
  BlockRemovalListener removalListener;
}
