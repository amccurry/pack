package pack.iscsi.volume.cache;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.block.BlockIOFactory;

@Value
@Builder
public class BlockRemovalListenerConfig {

  BlockIOFactory externalBlockStoreFactory;

}
