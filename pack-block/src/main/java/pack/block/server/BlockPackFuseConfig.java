package pack.block.server;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import pack.block.server.admin.BlockPackAdmin;

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
  String zkConnectionString;
  int zkSessionTimeout;
  boolean fileSystemMount;
  BlockStoreFactory blockStoreFactory;
  String volumeName;
  int maxVolumeMissingCount;
  int maxNumberOfMountSnapshots;
  long volumeMissingPollingPeriod;
  boolean countDockerDownAsMissing;
  HdfsSnapshotStrategy strategy;
}
