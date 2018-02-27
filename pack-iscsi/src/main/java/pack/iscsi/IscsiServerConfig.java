package pack.iscsi;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.Builder;
import lombok.Value;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;

@Value
@Builder
public class IscsiServerConfig {

  Set<String> addresses;
  int port;
  TargetManager iscsiTargetManager;
  Configuration configuration;
  Path root;
  File cacheDir;
  UserGroupInformation ugi;
  String serialId;
  List<String> brokerServers;
  HdfsBlockStoreConfig hdfsBlockStoreConfig;
  boolean hdfsStorageModuleEnabled;

}
