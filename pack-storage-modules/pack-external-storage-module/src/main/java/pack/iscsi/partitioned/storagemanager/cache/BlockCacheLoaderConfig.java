package pack.iscsi.partitioned.storagemanager.cache;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
import pack.iscsi.partitioned.storagemanager.VolumeStore;
import pack.iscsi.partitioned.storagemanager.BlockIOFactory;

@Value
@Builder
public class BlockCacheLoaderConfig {
  VolumeStore volumeStore;
  BlockStore blockStore;
  BlockWriteAheadLog writeAheadLog;
  File blockDataDir;
  BlockIOFactory externalBlockStoreFactory;
  long syncTimeAfterIdle;
  TimeUnit syncTimeAfterIdleTimeUnit;
  BlockRemovalListener removalListener;
}
