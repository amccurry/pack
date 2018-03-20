package pack.iscsi.docker.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class MountUnmountRequest {

  @JsonProperty("Name")
  String volumeName;

  @JsonProperty("ID")
  String id;

}
