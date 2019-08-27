package pack.iscsi.partitioned.storagemanager;

import java.io.File;

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
}
