package pack.iscsi.partitioned.storagemanager;

import java.io.File;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.external.ExternalBlockIOFactory;

@Value
@Builder
public class BlockStorageModuleFactoryConfig {

  BlockStore blockStore;

  BlockWriteAheadLog writeAheadLog;

  ExternalBlockIOFactory externalBlockStoreFactory;

  File blockDataDir;

  long maxCacheSizeInBytes;

  @Builder.Default
  long syncTimeAfterIdle = 1;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.MINUTES;

  @Builder.Default
  int syncThreads = ForkJoinPool.getCommonPoolParallelism();
}
