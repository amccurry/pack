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
                                                                                .cacheMaxMemorySoft(16 * 1024 * 1024)
                                                                                .cacheMaxMemoryHard(32 * 1024 * 1024)
                                                                                .cacheMaxMemoryEntriesSoft(16 * 1024)
                                                                                .cacheMaxMemoryEntriesHard(32 * 1024)
                                                                                .blockFilePeriod(15)
                                                                                .blockFileUnit(TimeUnit.SECONDS)
                                                                                .build();

  int cacheMaxMemoryHard;

  int cacheMaxMemoryEntriesHard;

  int cacheMaxMemorySoft;

  int cacheMaxMemoryEntriesSoft;

  long blockFilePeriod;

  TimeUnit blockFileUnit;

}
