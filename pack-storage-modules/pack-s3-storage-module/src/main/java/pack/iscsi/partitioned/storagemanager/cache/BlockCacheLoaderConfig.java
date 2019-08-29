package pack.iscsi.partitioned.storagemanager.cache;

import java.io.File;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;

@Value
@Builder
public class BlockCacheLoaderConfig {
  BlockStore blockStore;
  BlockWriteAheadLog writeAheadLog;
  File blockDataDir;
  ExternalBlockIOFactory externalBlockStoreFactory;
  long syncTimeAfterIdle;
  TimeUnit syncTimeAfterIdleTimeUnit;
  BlockRemovalListener removalListener;
}
