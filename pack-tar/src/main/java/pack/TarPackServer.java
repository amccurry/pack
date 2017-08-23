package pack;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class TarPackServer extends PackServer {

  private static final String VAR_LIB_PACK = "/var/lib/pack";

  private static final String PACK_HDFS_PATH = "PACK_HDFS_PATH";
  private static final String PACK_HDFS_USER = "PACK_HDFS_USER";
  private static final String PACK_LOCAL = "PACK_LOCAL";
  private static final String PACK_SCOPE = "PACK_SCOPE";

  public static void main(String[] args) throws Exception {
    File localFile = new File(getLocalCachePath());
    Path remotePath = new Path(getHdfsPath());
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(getHdfsUser());

    String sockerFile = "/run/docker/plugins/tarpack.sock";

    TarPackServer packServer = new TarPackServer(isGlobal(), sockerFile, localFile, remotePath, ugi);
    packServer.runServer();
  }

  private static boolean isGlobal() {
    String v = System.getenv(PACK_SCOPE);
    if (v != null && "global".equals(v.toLowerCase())) {
      return true;
    }
    return false;
  }

  private static String getHdfsUser() {
    String v = System.getenv(PACK_HDFS_USER);
    if (v == null) {
      throw new RuntimeException("Hdfs user not configured [" + PACK_HDFS_USER + "].");
    }
    return v;
  }

  private static String getHdfsPath() {
    String v = System.getenv(PACK_HDFS_PATH);
    if (v == null) {
      throw new RuntimeException("Hdfs path not configured [" + PACK_HDFS_PATH + "].");
    }
    return v;
  }

  private static String getLocalCachePath() {
    String v = System.getenv(PACK_LOCAL);
    if (v == null) {
      return VAR_LIB_PACK;
    }
    return v;
  }

  private final File localFile;
  private final Path remotePath;
  private final UserGroupInformation ugi;
  private final Configuration configuration = new Configuration();

  public TarPackServer(boolean global, String sockFile, File localFile, Path remotePath, UserGroupInformation ugi) {
    super(global, sockFile);
    this.localFile = localFile;
    this.remotePath = remotePath;
    this.ugi = ugi;
    localFile.mkdirs();
  }

  @Override
  protected PackStorage getPackStorage() throws Exception {
    return new TarPackStorage(localFile, configuration, remotePath, ugi);
  }

}
