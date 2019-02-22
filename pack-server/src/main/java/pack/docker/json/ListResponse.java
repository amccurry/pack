package pack.docker.json;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class ListResponse {

  @JsonProperty("Volumes")
  List<Volume> volumes;

  @JsonProperty("Err")
  String error;
}
