package pack.iscsi.spi;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@Builder
@EqualsAndHashCode
public class BlockKey {

  long volumeId;
  
  long blockId;

}
