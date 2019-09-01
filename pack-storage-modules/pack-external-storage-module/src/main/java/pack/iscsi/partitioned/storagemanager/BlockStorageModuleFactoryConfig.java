package pack.iscsi.partitioned.storagemanager;

import java.io.File;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BlockStorageModuleFactoryConfig {

  VolumeStore volumeStore;
  
  BlockStore blockStore;

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
}
