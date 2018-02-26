package pack.iscsi.storage;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class OffsetInfo {
  long offset;
  long timestamp;
}