package pack.block.server;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.PackStorage;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import pack.block.blockstore.hdfs.util.LastestHdfsSnapshotStrategy;
import pack.block.server.BlockPackStorageConfig.BlockPackStorageConfigBuilder;
import pack.block.util.Utils;
import pack.docker.PackServer;
import pack.util.ExecUtil;
import pack.util.LogLevel;
import spark.Service;

public class BlockPackServer extends PackServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackServer.class);

  private static final String _700 = "700";
  private static final String CHMOD = "chmod";
  private static final String SUDO = "sudo";
  private static final String R = "-R";
  private static final String P = "-p";
  private static final String MKDIR = "mkdir";
  private static final String CHOWN = "chown";
  private static final String RUN_DOCKER = "/run/docker";
  private static final String ROOT_ROOT = "root:root";
  private static final String RUN_DOCKER_PLUGINS = RUN_DOCKER + "/plugins";

  public static void main(String[] args) throws Exception {
    Utils.setupLog4j();
    File localWorkingDir = new File(Utils.getLocalWorkingPath());
    LOGGER.info("localWorkingDir {}", localWorkingDir);
    File localLogDir = new File(Utils.getLocalLogPath());
    LOGGER.info("localLogDir {}", localLogDir);
    Path remotePath = new Path(Utils.getHdfsPath());
    LOGGER.info("remotePath {}", remotePath);
    int numberOfMountSnapshots = Utils.getNumberOfMountSnapshots();
    LOGGER.info("numberOfMountSnapshots {}", numberOfMountSnapshots);
    HdfsSnapshotStrategy strategy = getStrategy();

    setupDockerDirs();
    String sockerFile = RUN_DOCKER_PLUGINS + "/pack.sock";

    BlockPackServer packServer = new BlockPackServer(true, sockerFile, localWorkingDir, localLogDir,
        remotePath, numberOfMountSnapshots, strategy);
    packServer.runServer();
  }

  private static void setupDockerDirs() throws IOException {
    ExecUtil.exec(LOGGER, LogLevel.DEBUG, SUDO, MKDIR, P, RUN_DOCKER_PLUGINS);
    ExecUtil.exec(LOGGER, LogLevel.DEBUG, SUDO, CHOWN, R, ROOT_ROOT, RUN_DOCKER);
    ExecUtil.exec(LOGGER, LogLevel.DEBUG, SUDO, CHMOD, _700, RUN_DOCKER);
    ExecUtil.exec(LOGGER, LogLevel.DEBUG, SUDO, CHMOD, _700, RUN_DOCKER_PLUGINS);
  }

  public static HdfsSnapshotStrategy getStrategy() {
    return new LastestHdfsSnapshotStrategy();
  }

  private final File _localWorkingDir;
  private final File _localLogDir;
  private final Path _remotePath;
  private final Configuration configuration = new Configuration();
  private final int _numberOfMountSnapshots;
  private final HdfsSnapshotStrategy _strategy;

  public BlockPackServer(boolean global, String sockFile, File localWorkingDir, File localLogDir, Path remotePath,
      int numberOfMountSnapshots, HdfsSnapshotStrategy strategy) {
    super(global, sockFile);
    _strategy = strategy;
    _numberOfMountSnapshots = numberOfMountSnapshots;
    _localWorkingDir = localWorkingDir;
    _localLogDir = localLogDir;
    _remotePath = remotePath;
    localWorkingDir.mkdirs();
    localLogDir.mkdirs();
  }

  @Override
  protected PackStorage getPackStorage(Service service) throws Exception {
    BlockPackStorageConfigBuilder builder = BlockPackStorageConfig.builder();
    builder.configuration(configuration)
           .remotePath(_remotePath)
           .logDir(_localLogDir)
           .workingDir(_localWorkingDir)
           .numberOfMountSnapshots(_numberOfMountSnapshots)
           .strategy(_strategy)
           .service(service)
           .build();
    return new BlockPackStorage(builder.build());
  }

}
