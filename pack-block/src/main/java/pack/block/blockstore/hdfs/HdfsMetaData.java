package pack.block.blockstore.hdfs;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class HdfsMetaData {

  @JsonProperty
  long length;

}
