package pack.block.server.webapp.response;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class ListVolumeResponse {
  List<String> volumes;
}
