package pack.block.server;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class BlockPackStorageConfig {
  File workingDir;
  File logDir;
  Configuration configuration;
  Path remotePath;
  UserGroupInformation ugi;
  String zkConnection;
  int zkTimeout;
  int numberOfMountSnapshots;
  long volumeMissingPollingPeriod;
  int volumeMissingCountBeforeAutoShutdown;
  boolean countDockerDownAsMissing;
  boolean nohupProcess;
  boolean fileSystemMount;
}
