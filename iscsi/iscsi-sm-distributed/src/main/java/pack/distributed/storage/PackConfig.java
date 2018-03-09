package pack.distributed.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.iscsi.storage.utils.PackUtils;

public class PackConfig {

  private static final String PACK_ISCSI_SERIAL_ID = "PACK_ISCSI_SERIAL_ID";
  private static final String WAL_CACHE_DIR = "WAL_CACHE_DIR";
  private static final String KAFKA_ZK_CONNECTION = "KAFKA_ZK_CONNECTION";
  private static final String HDFS_TARGET_PATH = "HDFS_TARGET_PATH";
  private static final String HDFS_CONF_PATH = "HDFS_CONF_PATH";
  private static final String XML = ".xml";
  private static final String HDFS_KERBEROS_KEYTAB = "HDFS_KERBEROS_KEYTAB";
  private static final String HDFS_KERBEROS_PRINCIPAL = "HDFS_KERBEROS_PRINCIPAL";
  private static final String HDFS_UGI_REMOTE_USER = "HDFS_UGI_REMOTE_USER";
  private static final String HDFS_UGI_CURRENT_USER = "HDFS_UGI_CURRENT_USER";

  public static Path getHdfsTarget() {
    return new Path(PackUtils.getEnvFailIfMissing(HDFS_TARGET_PATH));
  }

  public static UserGroupInformation getUgi() throws IOException {
    UserGroupInformation.setConfiguration(getConfiguration());
    if (PackUtils.isEnvSet(HDFS_UGI_CURRENT_USER)) {
      return UserGroupInformation.getCurrentUser();
    }
    String remoteUser = PackUtils.getEnv(HDFS_UGI_REMOTE_USER);
    if (remoteUser != null) {
      return UserGroupInformation.createRemoteUser(remoteUser);
    }
    String user = PackUtils.getEnv(HDFS_KERBEROS_PRINCIPAL);
    if (user != null) {
      String path = PackUtils.getEnvFailIfMissing(HDFS_KERBEROS_KEYTAB);
      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, path);
    }
    return UserGroupInformation.getLoginUser();
  }

  public static Configuration getConfiguration() throws IOException {
    String configPath = PackUtils.getEnvFailIfMissing(HDFS_CONF_PATH);
    Configuration configuration = new Configuration();
    File file = new File(configPath);
    if (file.isDirectory()) {
      File[] listFiles = file.listFiles((FilenameFilter) (dir, name) -> name.endsWith(XML));
      for (File f : listFiles) {
        configuration.addResource(new FileInputStream(f));
      }
    }
    return configuration;
  }

  public static String getKafkaZkConnection() {
    return PackUtils.getEnvFailIfMissing(KAFKA_ZK_CONNECTION);
  }

  public static File getWalCachePath() {
    return new File(PackUtils.getEnvFailIfMissing(WAL_CACHE_DIR));
  }

  public static String getPackSerialId() {
    return PackUtils.getEnvFailIfMissing(PACK_ISCSI_SERIAL_ID);
  }
}
