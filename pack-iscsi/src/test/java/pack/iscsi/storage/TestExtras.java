package pack.iscsi.storage;

import java.util.concurrent.TimeUnit;

public interface TestExtras {

  default PackStorageMetaData getMetaData(int blockSize, String testName) {
    PackStorageMetaData metaData = PackStorageMetaData.builder()
                                                      .blockSize(blockSize)
                                                      .kafkaPartition(0)
                                                      .kafkaTopic(testName)
                                                      .lengthInBytes(100 * blockSize)
                                                      .localWalCachePath("./target/test/" + testName)
                                                      .walCacheMemorySize(1024 * blockSize)
                                                      .maxOffsetPerWalFile(10)
                                                      .lagSyncPollWaitTime(1)
                                                      .hdfsPollTime(TimeUnit.SECONDS.toMillis(10))
                                                      .build();
    return metaData;
  }

}
