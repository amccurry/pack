package pack.block.blockstore.hdfs;

import org.apache.hadoop.fs.Path;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class CreateVolumeRequest {
  HdfsMetaData metaData;
  Path volumePath;
  Path clonePath;
  String volumeName;
  boolean symlinkClone;
}
