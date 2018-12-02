package pack.docker.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class GetResponse {
  
  @JsonProperty("Volume")
  Volume volume;

  @JsonProperty("Err")
  String error;
}
