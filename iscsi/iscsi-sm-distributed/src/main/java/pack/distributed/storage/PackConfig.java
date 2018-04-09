package pack.distributed.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import pack.distributed.storage.wal.WalFactoryType;
import pack.distributed.storage.zk.PackZooKeeperServerConfig;
import pack.iscsi.storage.utils.PackUtils;

public class PackConfig {

  private static final String ZK_DIR = "ZK_DIR";
  public static final String SERVER_ID = "SERVER_ID";
  public static final String SERVER_LEADER_ELECT_PORT = "SERVER_LEADER_ELECT_PORT";
  public static final String SERVER_PEER_PORT = "SERVER_PEER_PORT";
  public static final String SERVER_CLIENT_PORT = "SERVER_CLIENT_PORT";
  public static final String SERVER_ID_LIST = "SERVER_ID_LIST";
  public static final String HDFS_BLOCK_GC_DELAY = "HDFS_BLOCK_GC_DELAY";
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
  public static final long HDFS_BLOCK_GC_DELAY_DEAULT = TimeUnit.MINUTES.toMillis(10);
  public static final String PACK_ISCSI_ADDRESS = "PACK_ISCSI_ADDRESS";
  public static final String PACK_HTTP_ADDRESS = "PACK_HTTP_ADDRESS";
  public static final String PACK_HTTP_ADDRESS_DEFAULT = "0.0.0.0";
  public static final String PACK_HTTP_PORT_DEFAULT = "8642";
  public static final String PACK_HTTP_PORT = "PACK_HTTP_PORT";

  public static Path getHdfsTarget() {
    return new Path(PackUtils.getPropertyFailIfMissing(HDFS_TARGET_PATH));
  }

  public static UserGroupInformation getUgi() throws IOException {
    UserGroupInformation.setConfiguration(getConfiguration());
    if (PackUtils.isPropertySet(HDFS_UGI_CURRENT_USER)) {
      return UserGroupInformation.getCurrentUser();
    }
    String remoteUser = PackUtils.getProperty(HDFS_UGI_REMOTE_USER);
    if (remoteUser != null) {
      return UserGroupInformation.createRemoteUser(remoteUser);
    }
    String user = PackUtils.getProperty(HDFS_KERBEROS_PRINCIPAL);
    if (user != null) {
      String path = PackUtils.getPropertyFailIfMissing(HDFS_KERBEROS_KEYTAB);
      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, path);
    }
    return UserGroupInformation.getLoginUser();
  }

  public static Configuration getConfiguration() throws IOException {
    String configPath = PackUtils.getPropertyFailIfMissing(HDFS_CONF_PATH);
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
    return PackUtils.getProperty(KAFKA_ZK_CONNECTION);
  }

  public static File getWalCachePath() {
    return new File(PackUtils.getPropertyFailIfMissing(WAL_CACHE_DIR));
  }

  public static File getZkPath() {
    return new File(PackUtils.getPropertyFailIfMissing(ZK_DIR));
  }

  public static long getMaxWalSize() {
    return Long.parseLong(PackUtils.getProperty(WAL_MAX_SIZE, Long.toString(WAL_MAX_SIZE_DEFAULT)));
  }

  public static long getMaxWalLifeTime() {
    return Long.parseLong(PackUtils.getProperty(WAL_MAX_LIFE_TIME, Long.toString(WAL_MAX_LIFE_TIME_DEAULT)));
  }

  public static int getZooKeeperSessionTimeout() {
    return Integer.parseInt(PackUtils.getProperty(ZK_TIMEOUT, Integer.toString(ZK_TIMEOUT_DEAULT)));
  }

  public static String getZooKeeperConnection() {
    return PackUtils.getPropertyFailIfMissing(ZK_CONNECTION);
  }

  public static long getMaxBlockFileSize() {
    return Long.parseLong(PackUtils.getProperty(MAX_BLOCK_FILE_SIZE, Long.toString(MAX_BLOCK_FILE_SIZE_DEAULT)));
  }

  public static double getMaxObsoleteRatio() {
    return Double.parseDouble(PackUtils.getProperty(MAX_OBSOLETE_RATIO, Double.toString(MAX_OBSOLETE_RATIO_DEAULT)));
  }

  public static int getWriteBlockMonitorPort() {
    return Integer.parseInt(
        PackUtils.getProperty(WRITE_BLOCK_MONITOR_PORT, Integer.toString(WRITE_BLOCK_MONITOR_PORT_DEAULT)));
  }

  public static String getWriteBlockMonitorBindAddress() {
    return PackUtils.getProperty(WRITE_BLOCK_MONITOR_BIND_ADDRESS, WRITE_BLOCK_MONITOR_BIND_ADDRESS_DEFAULT);
  }

  public static String getWriteBlockMonitorAddress() {
    String address;
    try {
      address = InetAddress.getLocalHost()
                           .getHostAddress();
    } catch (UnknownHostException e) {
      return PackUtils.getPropertyFailIfMissing(WRITE_BLOCK_MONITOR_ADDRESS);
    }
    return PackUtils.getProperty(WRITE_BLOCK_MONITOR_ADDRESS, address);
  }

  public static long getHdfsBlockGCDelay() {
    return Long.parseLong(PackUtils.getProperty(HDFS_BLOCK_GC_DELAY, Long.toString(HDFS_BLOCK_GC_DELAY_DEAULT)));
  }

  public static String getHttpAddress() {
    return PackUtils.getProperty(PACK_HTTP_ADDRESS, PACK_HTTP_ADDRESS_DEFAULT);
  }

  public static int getHttpPort() {
    return Integer.parseInt(PackUtils.getProperty(PACK_HTTP_PORT, PACK_HTTP_PORT_DEFAULT));
  }

  public static PackZooKeeperServerConfig getPackZooKeeperServerConfig() {
    long serverId = Long.parseLong(PackUtils.getPropertyFailIfMissing(SERVER_ID));
    return createPackZooKeeperServerConfig(serverId);
  }

  private static PackZooKeeperServerConfig createPackZooKeeperServerConfig(long serverId) {
    return PackZooKeeperServerConfig.builder()
                                    .clientPort(
                                        Integer.parseInt(PackUtils.getPropertyFailIfMissing(SERVER_CLIENT_PORT)))
                                    .peerPort(Integer.parseInt(PackUtils.getPropertyFailIfMissing(SERVER_PEER_PORT)))
                                    .leaderElectPort(
                                        Integer.parseInt(PackUtils.getPropertyFailIfMissing(SERVER_LEADER_ELECT_PORT)))
                                    .id(serverId)
                                    .hostname(findHostName(serverId))
                                    .build();
  }

  private static String findHostName(long serverId) {
    Map<Long, String> serverToId = getServerToId();
    return serverToId.get(serverId);
  }

  private static Map<Long, String> getServerToId() {
    File file = new File(PackUtils.getPropertyFailIfMissing(SERVER_ID_LIST));
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
      String line;
      Builder<Long, String> builder = ImmutableMap.builder();
      while ((line = reader.readLine()) != null) {
        int indexOf1 = line.indexOf(':');
        int indexOf2 = line.indexOf('=');
        String server = line.substring(0, indexOf1);
        String id = line.substring(indexOf2 + 1);
        builder.put(Long.parseLong(id), server);
      }
      return builder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static List<PackZooKeeperServerConfig> getAllPackZooKeeperServerConfig() {
    Map<Long, String> serverToId = getServerToId();
    ImmutableList.Builder<PackZooKeeperServerConfig> builder = ImmutableList.builder();
    for (Entry<Long, String> e : serverToId.entrySet()) {
      builder.add(createPackZooKeeperServerConfig(e.getKey()));
    }
    return builder.build();
  }

  public static boolean isDataZkEmbedded() {
    return PackUtils.getProperty(ZK_DIR) == null;
  }

  public static WalFactoryType getBroadcastFactoryType() {
    // TODO Auto-generated method stub
    return null;
  }

  public static long getHdfsWalMaxTimeOpenForWriting(long defaultMaxOpenForWriting) {
    // TODO Auto-generated method stub
    return 0;
  }

  public static long getHdfsWalMaxAmountAllowedPerFile(int defaultMaxAmountAllowedPerFile) {
    // TODO Auto-generated method stub
    return 0;
  }

}
