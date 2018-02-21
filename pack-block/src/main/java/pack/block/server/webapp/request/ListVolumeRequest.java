package pack.block.server.webapp.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class ListVolumeRequest {

  String filter;

}
