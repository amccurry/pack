package pack.block.server;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.PackServer;
import pack.PackStorage;
import pack.block.util.Utils;

public class BlockPackServer extends PackServer {

  private static final String PACK_SCOPE = "PACK_SCOPE";

  public static void main(String[] args) throws Exception {
    Utils.setupLog4j();
    File localFile = new File(Utils.getLocalCachePath());
    Path remotePath = new Path(Utils.getHdfsPath());
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    String zkConnectionString = Utils.getZooKeeperConnectionString();
    int sessionTimeout = Utils.getZooKeeperConnectionTimeout();
    String sockerFile = "/run/docker/plugins/pack.sock";
    BlockPackServer packServer = new BlockPackServer(isGlobal(), sockerFile, localFile, remotePath, ugi,
        zkConnectionString, sessionTimeout);
    packServer.runServer();
  }

  private final File localFile;
  private final Path remotePath;
  private final UserGroupInformation ugi;
  private final Configuration configuration = new Configuration();
  private final String zkConnection;
  private final int zkTimeout;

  public BlockPackServer(boolean global, String sockFile, File localFile, Path remotePath, UserGroupInformation ugi,
      String zkConnection, int zkTimeout) {
    super(global, sockFile);
    this.localFile = localFile;
    this.remotePath = remotePath;
    this.ugi = ugi;
    this.zkConnection = zkConnection;
    this.zkTimeout = zkTimeout;
    localFile.mkdirs();
  }

  @Override
  protected PackStorage getPackStorage() throws Exception {
    return new BlockPackStorage(localFile, configuration, remotePath, ugi, zkConnection, zkTimeout);
  }

  private static boolean isGlobal() {
    String v = System.getenv(PACK_SCOPE);
    if (v != null && "global".equals(v.toLowerCase())) {
      return true;
    }
    return false;
  }
}
