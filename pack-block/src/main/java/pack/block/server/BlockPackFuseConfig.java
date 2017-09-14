package pack.block.server;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.server.admin.BlockPackAdmin;
import pack.zk.utils.ZooKeeperClient;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class BlockPackFuseConfig {
  BlockPackAdmin blockPackAdmin;
  UserGroupInformation ugi;
  FileSystem fileSystem;
  Path path;
  HdfsBlockStoreConfig config;
  String fuseLocalPath;
  String fsLocalPath;
  String metricsLocalPath;
  String fsLocalCache;
  ZooKeeperClient zooKeeper;
  boolean fileSystemMount;
}
