package pack.iscsi.docker.json;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class CreateRequest {
  @JsonProperty("Name")
  String volumeName;

  @JsonProperty("Opts")
  Map<String, Object> options;
}
