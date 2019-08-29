package pack.iscsi.partitioned.storagemanager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.LoadingCache;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.partitioned.block.Block;

@Value
@Builder
public class BlockStorageModuleConfig {

  LoadingCache<BlockKey, Block> globalCache;

  long volumeId;

  int blockSize;

  long lengthInBytes;

  ExternalBlockIOFactory externalBlockStoreFactory;

  @Builder.Default
  long syncTimeAfterIdle = 1;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.MINUTES;

  ExecutorService syncExecutor;
}
