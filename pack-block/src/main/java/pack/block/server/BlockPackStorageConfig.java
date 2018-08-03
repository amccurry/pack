package pack.block.server;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import spark.Service;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class BlockPackStorageConfig {
  File workingDir;
  File logDir;
  Configuration configuration;
  Path remotePath;
  int numberOfMountSnapshots;
  boolean nohupProcess;
  HdfsSnapshotStrategy strategy;
  Service service;
}
