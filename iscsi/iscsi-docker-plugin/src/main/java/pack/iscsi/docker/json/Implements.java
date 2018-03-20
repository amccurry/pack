package pack.iscsi.docker.json;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class Implements {

  @JsonProperty("Implements")
  List<String> impls;
}
