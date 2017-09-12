package pack.block.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.PackServer;
import pack.PackServer.Result;

public class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  private static final int PACK_ZOOKEEPER_CONNECTION_TIMEOUT_DEFAULT = 30000;
  private static final String PACK_ZOOKEEPER_CONNECTION_TIMEOUT = "PACK_ZOOKEEPER_CONNECTION_TIMEOUT";
  private static final String PACK_ZOOKEEPER_CONNECTION_STR = "PACK_ZOOKEEPER_CONNECTION_STR";
  public static final String VAR_LIB_PACK = "/var/lib/pack";
  public static final String PACK_HDFS_PATH = "PACK_HDFS_PATH";
  public static final String PACK_LOCAL = "PACK_LOCAL";
  public static final String PACK_LOG4J_CONFIG = "PACK_LOG4J_CONFIG";
  public static final String XML = ".xml";
  public static final String PACK_HDFS_KERBEROS_KEYTAB = "PACK_HDFS_KERBEROS_KEYTAB";
  public static final String PACK_HDFS_KERBEROS_PRINCIPAL_NAME = "PACK_HDFS_KERBEROS_PRINCIPAL_NAME";
  public static final String PACK_HDFS_USER = "PACK_HDFS_USER";

  public static void closeQuietly(final Closeable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (final IOException ioe) {
      // ignore
    }
  }

  public static void close(final Logger logger, final Closeable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (final IOException ioe) {
      logger.error("Unknown error while trying to close.", ioe);
    }
  }

  public static void setupLog4j() {
    String log4jConfigFile = System.getenv(PACK_LOG4J_CONFIG);
    if (log4jConfigFile == null) {
      return;
    } else if (log4jConfigFile.endsWith(XML)) {
      DOMConfigurator.configure(log4jConfigFile);
    } else {
      PropertyConfigurator.configure(log4jConfigFile);
    }
  }

  public static UserGroupInformation getUserGroupInformation() throws IOException {
    UserGroupInformation ugi;
    String hdfsPrinciaplName = getHdfsPrincipalName();
    String hdfsUser = getHdfsUser();
    if (hdfsPrinciaplName != null) {
      String hdfsKeytab = getHdfsKeytab();
      LOGGER.info("principal {} keytab location {}", hdfsPrinciaplName, hdfsKeytab);
      UserGroupInformation.loginUserFromKeytab(hdfsPrinciaplName, hdfsKeytab);
      ugi = UserGroupInformation.getLoginUser();
    } else if (hdfsUser == null) {
      ugi = UserGroupInformation.getCurrentUser();
    } else {
      ugi = UserGroupInformation.createRemoteUser(hdfsUser);
    }
    return ugi;
  }

  public static String getHdfsPrincipalName() {
    String v = System.getenv(PACK_HDFS_KERBEROS_PRINCIPAL_NAME);
    if (v == null) {
      return null;
    }
    return v;
  }

  public static String getHdfsKeytab() {
    String v = System.getenv(PACK_HDFS_KERBEROS_KEYTAB);
    if (v == null) {
      throw new RuntimeException("Keytab path not configured [" + PACK_HDFS_KERBEROS_KEYTAB + "].");
    }
    return v;
  }

  public static String getHdfsUser() {
    String v = System.getenv(PACK_HDFS_USER);
    if (v == null) {
      return null;
    }
    return v;
  }

  public static void loadConfigIfExists(Configuration configuration, String dirPath) throws IOException {
    if (dirPath != null) {
      File dir = new File(dirPath);
      File core = new File(dir, "core-site.xml");
      if (core.exists()) {
        configuration.addResource(new FileInputStream(core));
      }
      File hdfs = new File(dir, "hdfs-site.xml");
      if (hdfs.exists()) {
        configuration.addResource(new FileInputStream(hdfs));
      }
    }
  }

  public static String getHdfsPath() {
    String v = System.getenv(PACK_HDFS_PATH);
    if (v == null) {
      throw new RuntimeException("Hdfs path not configured [" + PACK_HDFS_PATH + "].");
    }
    return v;
  }

  public static String getLocalCachePath() {
    String v = System.getenv(PACK_LOCAL);
    if (v == null) {
      return VAR_LIB_PACK;
    }
    return v;
  }

  public static String getZooKeeperConnectionString() {
    String v = System.getenv(PACK_ZOOKEEPER_CONNECTION_STR);
    if (v == null) {
      throw new RuntimeException("ZooKeeper connection string not configured [" + PACK_ZOOKEEPER_CONNECTION_STR + "].");
    }
    return v;
  }

  public static int getZooKeeperConnectionTimeout() {
    String v = System.getenv(PACK_ZOOKEEPER_CONNECTION_TIMEOUT);
    if (v == null) {
      return PACK_ZOOKEEPER_CONNECTION_TIMEOUT_DEFAULT;
    }
    return Integer.parseInt(v);
  }

  public static String getLockName(Path volumePath) {
    String path = volumePath.toUri()
                            .getPath();
    return path.replaceAll("/", "__");
  }

  public static void exec(Logger logger, String... command) throws IOException {
    String uuid = UUID.randomUUID()
                      .toString();
    List<String> list = Arrays.asList(command);
    logger.info("Executing command id {} cmd {}", uuid, list);
    Result result;
    try {
      result = PackServer.exec(uuid, list, logger);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      logger.info("Command id {} complete", uuid);
    }
    if (result.exitCode != 0) {
      throw new IOException("Unknown error while trying to run command " + Arrays.asList(command));
    }
  }

  public static int execReturnExitCode(Logger logger, String... command) throws IOException {
    String uuid = UUID.randomUUID()
                      .toString();
    List<String> list = Arrays.asList(command);
    logger.info("Executing command id {} cmd {}", uuid, list);
    Result result;
    try {
      result = PackServer.exec(uuid, list, logger);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      logger.info("Command id {} complete", uuid);
    }
    return result.exitCode;
  }

  public static void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }
}
