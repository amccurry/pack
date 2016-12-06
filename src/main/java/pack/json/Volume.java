package pack.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class Volume {

  @JsonProperty("Name")
  String volumeName;

  @JsonProperty("Mountpoint")
  String mountpoint;

}
