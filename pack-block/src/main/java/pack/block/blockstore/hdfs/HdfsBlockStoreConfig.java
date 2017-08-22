package pack.block.blockstore.hdfs;

import java.util.concurrent.TimeUnit;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class HdfsBlockStoreConfig {

  public static final HdfsBlockStoreConfig DEFAULT_CONFIG = HdfsBlockStoreConfig.builder()
                                                                                .maxMemoryForCache(16 * 1024 * 1024)
                                                                                .fileSystemBlockSize(1024)
                                                                                .blockFilePeriod(15)
                                                                                .blockFileUnit(TimeUnit.SECONDS)
                                                                                .build();

  int fileSystemBlockSize;

  int maxMemoryForCache;

  long blockFilePeriod;

  TimeUnit blockFileUnit;

}
