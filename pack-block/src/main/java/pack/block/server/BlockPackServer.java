package pack.block.server;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.PackServer;
import pack.PackStorage;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import pack.block.blockstore.hdfs.util.LastestHdfsSnapshotStrategy;
import pack.block.blockstore.hdfs.util.TimeBasedHdfsSnapshotStrategy;
import pack.block.server.BlockPackStorageConfig.BlockPackStorageConfigBuilder;
import pack.block.util.Utils;
import spark.Service;

public class BlockPackServer extends PackServer {

  private static final String GLOBAL = "global";
  private static final String PACK_SCOPE = "PACK_SCOPE";

  public static void main(String[] args) throws Exception {
    Utils.setupLog4j();
    File localWorkingDir = new File(Utils.getLocalWorkingPath());
    File localLogDir = new File(Utils.getLocalLogPath());
    Path remotePath = new Path(Utils.getHdfsPath());
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    String zkConnectionString = Utils.getZooKeeperConnectionString();
    int sessionTimeout = Utils.getZooKeeperConnectionTimeout();
    int numberOfMountSnapshots = Utils.getNumberOfMountSnapshots();
    long volumeMissingPollingPeriod = Utils.getVolumeMissingPollingPeriod();
    int volumeMissingCountBeforeAutoShutdown = Utils.getVolumeMissingCountBeforeAutoShutdown();
    boolean countDockerDownAsMissing = Utils.getCountDockerDownAsMissing();
    boolean nohupProcess = Utils.getNohupProcess();
    boolean fileSystemMount = Utils.getFileSystemMount();
    HdfsSnapshotStrategy strategy = getStrategy();

    String sockerFile = "/run/docker/plugins/pack.sock";
    BlockPackServer packServer = new BlockPackServer(isGlobal(), sockerFile, localWorkingDir, localLogDir, remotePath,
        ugi, zkConnectionString, sessionTimeout, numberOfMountSnapshots, volumeMissingPollingPeriod,
        volumeMissingCountBeforeAutoShutdown, countDockerDownAsMissing, nohupProcess, fileSystemMount, strategy);
    packServer.runServer();
  }

  private static HdfsSnapshotStrategy getStrategy() {
    return new LastestHdfsSnapshotStrategy();
  }

  private final File _localWorkingDir;
  private final File _localLogDir;
  private final Path _remotePath;
  private final UserGroupInformation _ugi;
  private final Configuration configuration = new Configuration();
  private final String _zkConnection;
  private final int _zkTimeout;
  private final int _numberOfMountSnapshots;
  private final long _volumeMissingPollingPeriod;
  private final int _volumeMissingCountBeforeAutoShutdown;
  private final boolean _countDockerDownAsMissing;
  private final boolean _nohupProcess;
  private final boolean _fileSystemMount;
  private final HdfsSnapshotStrategy _strategy;

  public BlockPackServer(boolean global, String sockFile, File localWorkingDir, File localLogDir, Path remotePath,
      UserGroupInformation ugi, String zkConnection, int zkTimeout, int numberOfMountSnapshots,
      long volumeMissingPollingPeriod, int volumeMissingCountBeforeAutoShutdown, boolean countDockerDownAsMissing,
      boolean nohupProcess, boolean fileSystemMount, HdfsSnapshotStrategy strategy) {
    super(global, sockFile);
    _strategy = strategy;
    _nohupProcess = nohupProcess;
    _numberOfMountSnapshots = numberOfMountSnapshots;
    _volumeMissingPollingPeriod = volumeMissingPollingPeriod;
    _volumeMissingCountBeforeAutoShutdown = volumeMissingCountBeforeAutoShutdown;
    _countDockerDownAsMissing = countDockerDownAsMissing;
    _localWorkingDir = localWorkingDir;
    _localLogDir = localLogDir;
    _remotePath = remotePath;
    _ugi = ugi;
    _zkConnection = zkConnection;
    _zkTimeout = zkTimeout;
    _fileSystemMount = fileSystemMount;
    localWorkingDir.mkdirs();
    localLogDir.mkdirs();
  }

  @Override
  protected PackStorage getPackStorage(Service service) throws Exception {
    BlockPackStorageConfigBuilder builder = BlockPackStorageConfig.builder();
    builder.ugi(_ugi)
           .configuration(configuration)
           .remotePath(_remotePath)
           .zkConnection(_zkConnection)
           .zkTimeout(_zkTimeout)
           .logDir(_localLogDir)
           .workingDir(_localWorkingDir)
           .numberOfMountSnapshots(_numberOfMountSnapshots)
           .volumeMissingPollingPeriod(_volumeMissingPollingPeriod)
           .nohupProcess(_nohupProcess)
           .countDockerDownAsMissing(_countDockerDownAsMissing)
           .volumeMissingCountBeforeAutoShutdown(_volumeMissingCountBeforeAutoShutdown)
           .fileSystemMount(_fileSystemMount)
           .strategy(_strategy)
           .service(service)
           .build();
    return new BlockPackStorage(builder.build());
  }

  private static boolean isGlobal() {
    String v = System.getenv(PACK_SCOPE);
    if (v != null && GLOBAL.equals(v.toLowerCase())) {
      return true;
    }
    return false;
  }
}
