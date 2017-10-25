package pack.block.server;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.PackServer;
import pack.PackStorage;
import pack.block.util.Utils;

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

    String sockerFile = "/run/docker/plugins/pack.sock";
    BlockPackServer packServer = new BlockPackServer(isGlobal(), sockerFile, localWorkingDir, localLogDir, remotePath,
        ugi, zkConnectionString, sessionTimeout, numberOfMountSnapshots, volumeMissingPollingPeriod,
        volumeMissingCountBeforeAutoShutdown, countDockerDownAsMissing);
    packServer.runServer();
  }

  private final File localWorkingDir;
  private final File localLogDir;
  private final Path remotePath;
  private final UserGroupInformation ugi;
  private final Configuration configuration = new Configuration();
  private final String zkConnection;
  private final int zkTimeout;
  private final int _numberOfMountSnapshots;
  private final long _volumeMissingPollingPeriod;
  private final int _volumeMissingCountBeforeAutoShutdown;
  private final boolean _countDockerDownAsMissing;

  public BlockPackServer(boolean global, String sockFile, File localWorkingDir, File localLogDir, Path remotePath,
      UserGroupInformation ugi, String zkConnection, int zkTimeout, int numberOfMountSnapshots,
      long volumeMissingPollingPeriod, int volumeMissingCountBeforeAutoShutdown, boolean countDockerDownAsMissing) {
    super(global, sockFile);
    _numberOfMountSnapshots = numberOfMountSnapshots;
    _volumeMissingPollingPeriod = volumeMissingPollingPeriod;
    _volumeMissingCountBeforeAutoShutdown = volumeMissingCountBeforeAutoShutdown;
    _countDockerDownAsMissing = countDockerDownAsMissing;
    this.localWorkingDir = localWorkingDir;
    this.localLogDir = localLogDir;
    this.remotePath = remotePath;
    this.ugi = ugi;
    this.zkConnection = zkConnection;
    this.zkTimeout = zkTimeout;
    localWorkingDir.mkdirs();
    localLogDir.mkdirs();
  }

  @Override
  protected PackStorage getPackStorage() throws Exception {
    return new BlockPackStorage(localWorkingDir, localLogDir, configuration, remotePath, ugi, zkConnection, zkTimeout,
        _numberOfMountSnapshots, _volumeMissingPollingPeriod, _volumeMissingCountBeforeAutoShutdown,
        _countDockerDownAsMissing);
  }

  private static boolean isGlobal() {
    String v = System.getenv(PACK_SCOPE);
    if (v != null && GLOBAL.equals(v.toLowerCase())) {
      return true;
    }
    return false;
  }
}
