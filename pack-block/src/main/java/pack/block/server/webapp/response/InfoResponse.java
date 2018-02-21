package pack.block.server.webapp.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import pack.block.blockstore.hdfs.HdfsMetaData;

@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class InfoResponse {
  
  @JsonProperty
  String error;
  
  @JsonProperty
  String name;
  
  @JsonProperty
  HdfsMetaData options;

}
