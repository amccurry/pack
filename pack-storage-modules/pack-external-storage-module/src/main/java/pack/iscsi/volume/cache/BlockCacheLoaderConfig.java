package pack.iscsi.volume.cache;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.volume.BlockGenerationStore;
import pack.iscsi.volume.BlockIOFactory;
import pack.iscsi.volume.VolumeStore;

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
