package pack.block.server.admin;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class ShutdownResponse {
  boolean success = true;
}
