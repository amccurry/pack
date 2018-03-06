package pack.distributed.storage.kafka.util;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaBrokerEntry {
  
  @JsonProperty
  Integer jmx_port;
  
  @JsonProperty
  String timestamp;
  
  @JsonProperty
  List<String> endpoints;
  
  @JsonProperty
  String host;
  
  @JsonProperty
  Long version;
  
  @JsonProperty
  Integer port;
}
