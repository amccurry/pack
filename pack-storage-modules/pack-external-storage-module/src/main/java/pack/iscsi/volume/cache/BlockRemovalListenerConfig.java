package pack.iscsi.volume.cache;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.volume.BlockIOFactory;

@Value
@Builder
public class BlockRemovalListenerConfig {

  BlockIOFactory externalBlockStoreFactory;

}
