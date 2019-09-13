package pack.iscsi.volume;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.LoadingCache;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.block.Block;

@Value
@Builder
public class BlockStorageModuleConfig {

  LoadingCache<BlockKey, Block> globalCache;
  
  String volumeName;

  long volumeId;

  int blockSize;

  long lengthInBytes;

  BlockIOFactory externalBlockStoreFactory;

  @Builder.Default
  long syncTimeAfterIdle = 1;

  @Builder.Default
  TimeUnit syncTimeAfterIdleTimeUnit = TimeUnit.MINUTES;

  ExecutorService syncExecutor;
  
  MetricRegistry metrics;
}
