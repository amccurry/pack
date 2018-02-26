package pack.iscsi.storage;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PackStorageMetaData {
  int blockSize;
  String kafkaTopic;
  Integer kafkaPartition;
  int maxOffsetPerWalFile;
  long lengthInBytes;
  long walCacheMemorySize;
  String localWalCachePath;
  long maxOffsetLagDiff;
  long lagSyncPollWaitTime;
  long hdfsPollTime;
}
