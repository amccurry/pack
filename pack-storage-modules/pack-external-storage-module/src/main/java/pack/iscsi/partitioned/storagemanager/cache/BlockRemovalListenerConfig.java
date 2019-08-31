package pack.iscsi.partitioned.storagemanager.cache;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.partitioned.storagemanager.BlockIOFactory;

@Value
@Builder
public class BlockRemovalListenerConfig {

  BlockIOFactory externalBlockStoreFactory;

}
