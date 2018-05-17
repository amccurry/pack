package pack.block.server.json;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import pack.block.server.BlockStoreFactory;
import pack.block.server.admin.BlockPackAdmin;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class BlockPackFuseConfigInternal {
  BlockPackFuseConfig blockPackFuseConfig;
  BlockPackAdmin blockPackAdmin;
  UserGroupInformation ugi;
  FileSystem fileSystem;
  Path path;
  HdfsBlockStoreConfig config;
  BlockStoreFactory blockStoreFactory;
  HdfsSnapshotStrategy strategy;
}
