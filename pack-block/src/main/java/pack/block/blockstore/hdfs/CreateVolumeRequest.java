package pack.block.blockstore.hdfs;

import org.apache.hadoop.fs.Path;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import pack.block.blockstore.BlockStoreMetaData;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class CreateVolumeRequest {
  BlockStoreMetaData metaData;
  Path volumePath;
  Path clonePath;
  String volumeName;
  boolean symlinkClone;
}
