package pack.iscsi.partitioned.storagemanager.cache;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.external.ExternalBlockIOFactory;

@Value
@Builder
public class BlockRemovalListenerConfig {

  ExternalBlockIOFactory externalBlockStoreFactory;

}
