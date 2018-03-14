package pack.distributed.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.iscsi.storage.utils.PackUtils;

public class PackConfig {

  public static final String WRITE_BLOCK_MONITOR_BIND_ADDRESS = "WRITE_BLOCK_MONITOR_BIND_ADDRESS";
  public static final String WRITE_BLOCK_MONITOR_ADDRESS = "WRITE_BLOCK_MONITOR_ADDRESS";
  public static final String MAX_BLOCK_FILE_SIZE = "MAX_BLOCK_FILE_SIZE";
  public static final String MAX_OBSOLETE_RATIO = "MAX_OBSOLETE_RATIO";
  public static final String WRITE_BLOCK_MONITOR_PORT = "WRITE_BLOCK_MONITOR_PORT";
  public static final String ZK_CONNECTION = "ZK_CONNECTION";
  public static final String ZK_TIMEOUT = "ZK_TIMEOUT";
  public static final long WAL_MAX_LIFE_TIME_DEAULT = TimeUnit.MINUTES.toMillis(1);
  public static final int WAL_MAX_SIZE_DEFAULT = 10_000_000;
  public static final String WAL_MAX_LIFE_TIME = "WAL_MAX_LIFE_TIME";
  public static final String WAL_MAX_SIZE = "WAL_MAX_SIZE";
  public static final String WAL_CACHE_DIR = "WAL_CACHE_DIR";
  public static final String KAFKA_ZK_CONNECTION = "KAFKA_ZK_CONNECTION";
  public static final String HDFS_TARGET_PATH = "HDFS_TARGET_PATH";
  public static final String HDFS_CONF_PATH = "HDFS_CONF_PATH";
  public static final String XML = ".xml";
  public static final String HDFS_KERBEROS_KEYTAB = "HDFS_KERBEROS_KEYTAB";
  public static final String HDFS_KERBEROS_PRINCIPAL = "HDFS_KERBEROS_PRINCIPAL";
  public static final String HDFS_UGI_REMOTE_USER = "HDFS_UGI_REMOTE_USER";
  public static final String HDFS_UGI_CURRENT_USER = "HDFS_UGI_CURRENT_USER";
  public static final int ZK_TIMEOUT_DEAULT = 30000;
  public static final long MAX_BLOCK_FILE_SIZE_DEAULT = 10_737_418_240L;
  public static final double MAX_OBSOLETE_RATIO_DEAULT = 0.5;
  public static final int WRITE_BLOCK_MONITOR_PORT_DEAULT = 9753;
  public static final String WRITE_BLOCK_MONITOR_BIND_ADDRESS_DEFAULT = "0.0.0.0";
  public static final int SERVER_STATUS_PORT_DEAULT = 9753;

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

  public static long getMaxWalSize() {
    return Long.parseLong(PackUtils.getEnv(WAL_MAX_SIZE, Long.toString(WAL_MAX_SIZE_DEFAULT)));
  }

  public static long getMaxWalLifeTime() {
    return Long.parseLong(PackUtils.getEnv(WAL_MAX_LIFE_TIME, Long.toString(WAL_MAX_LIFE_TIME_DEAULT)));
  }

  public static int getZooKeeperSessionTimeout() {
    return Integer.parseInt(PackUtils.getEnv(ZK_TIMEOUT, Integer.toString(ZK_TIMEOUT_DEAULT)));
  }

  public static String getZooKeeperConnection() {
    return PackUtils.getEnvFailIfMissing(ZK_CONNECTION);
  }

  public static long getMaxBlockFileSize() {
    return Long.parseLong(PackUtils.getEnv(MAX_BLOCK_FILE_SIZE, Long.toString(MAX_BLOCK_FILE_SIZE_DEAULT)));
  }

  public static double getMaxObsoleteRatio() {
    return Double.parseDouble(PackUtils.getEnv(MAX_OBSOLETE_RATIO, Double.toString(MAX_OBSOLETE_RATIO_DEAULT)));
  }

  public static int getWriteBlockMonitorPort() {
    return Integer.parseInt(
        PackUtils.getEnv(WRITE_BLOCK_MONITOR_PORT, Integer.toString(WRITE_BLOCK_MONITOR_PORT_DEAULT)));
  }

  public static String getWriteBlockMonitorBindAddress() {
    return PackUtils.getEnv(WRITE_BLOCK_MONITOR_BIND_ADDRESS, WRITE_BLOCK_MONITOR_BIND_ADDRESS_DEFAULT);
  }

  public static String getWriteBlockMonitorAddress() {
    String address;
    try {
      address = InetAddress.getLocalHost()
                           .getHostAddress();
    } catch (UnknownHostException e) {
      return PackUtils.getEnvFailIfMissing(WRITE_BLOCK_MONITOR_ADDRESS);
    }
    return PackUtils.getEnv(WRITE_BLOCK_MONITOR_ADDRESS, address);
  }

}
