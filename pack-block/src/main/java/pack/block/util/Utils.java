package pack.block.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.io.netty.util.internal.ThreadLocalRandom;
import com.google.common.base.Splitter;

import pack.PackServer;
import pack.PackServer.Result;
import pack.block.server.BlockPackFuse;
import pack.zk.utils.ZkUtils;
import pack.zk.utils.ZooKeeperClientFactory;
import sun.misc.Unsafe;

public class Utils {

  private static final String THE_UNSAFE = "theUnsafe";

  private static final String PACK_HDFS_KERBEROS_RELOGIN_INTERVAL = "PACK_HDFS_KERBEROS_RELOGIN_INTERVAL";

  public interface TimerWithException<T, E extends Throwable> {
    T time() throws E;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  public static final String HDFS_SITE_XML = "hdfs-site.xml";
  public static final String CORE_SITE_XML = "core-site.xml";
  public static final String PACK_FILE_SYSTEM_MOUNT = "PACK_FILE_SYSTEM_MOUNT";
  public static final String PACK_NOHUP_PROCESS = "PACK_NOHUP_PROCESS";
  public static final String GLOBAL = "global";
  public static final String PACK_SCOPE = "PACK_SCOPE";
  public static final int PACK_ZOOKEEPER_CONNECTION_TIMEOUT_DEFAULT = 30000;
  public static final String PACK_ZOOKEEPER_CONNECTION_TIMEOUT = "PACK_ZOOKEEPER_CONNECTION_TIMEOUT";
  public static final String PACK_ZOOKEEPER_CONNECTION_STR = "PACK_ZOOKEEPER_CONNECTION_STR";
  public static final String PACK_LOG = "PACK_LOG";
  public static final String PACK_HDFS_PATH = "PACK_HDFS_PATH";
  public static final String PACK_LOCAL = "PACK_LOCAL";
  public static final String PACK_LOG4J_CONFIG = "PACK_LOG4J_CONFIG";
  public static final String XML = ".xml";
  public static final String PACK_HDFS_KERBEROS_KEYTAB = "PACK_HDFS_KERBEROS_KEYTAB";
  public static final String PACK_HDFS_KERBEROS_PRINCIPAL_NAME = "PACK_HDFS_KERBEROS_PRINCIPAL_NAME";
  public static final String PACK_HDFS_USER = "PACK_HDFS_USER";
  public static final String VAR_LOG_PACK = "/var/log/pack";
  public static final String VAR_LIB_PACK = "/var/lib/pack";
  public static final String PACK_VOLUME_MISSING_COUNT_BEFORE_AUTO_SHUTDOWN = "PACK_VOLUME_MISSING_COUNT_BEFORE_AUTO_SHUTDOWN";
  public static final String PACK_VOLUME_MISSING_POLLING_PERIOD = "PACK_VOLUME_MISSING_POLLING_PERIOD";
  public static final String PACK_NUMBER_OF_MOUNT_SNAPSHOTS = "PACK_NUMBER_OF_MOUNT_SNAPSHOTS";
  public static final String PACK_COUNT_DOCKER_DOWN_AS_MISSING = "PACK_COUNT_DOCKER_DOWN_AS_MISSING";
  public static final boolean PACK_COUNT_DOCKER_DOWN_AS_MISSING_DEFAULT = true;
  public static final int PACK_VOLUME_MISSING_COUNT_BEFORE_AUTO_SHUTDOWN_DEFAULT = 2;
  public static final long PACK_VOLUME_MISSING_POLLING_PERIOD_DEFAULT = TimeUnit.SECONDS.toMillis(2);
  public static final int PACK_NUMBER_OF_MOUNT_SNAPSHOTS_DEFAULT = 5;

  private static final Splitter SPACE_SPLITTER = Splitter.on(' ');
  private static final String LS = "ls";
  private static final String ALLOCATED_SIZE_SWITCH = "-s";
  private static final String LENGTH_SWTICH = "--length";
  private static final String OFFSET_SWITCH = "--offset";
  private static final String PUNCH_HOLE_SWITCH = "--punch-hole";
  private static final String KEEP_SIZE_SWITCH = "--keep-size";
  private static final String FALLOCATE = "fallocate";
  private static final AtomicReference<UserGroupInformation> UGI = new AtomicReference<>();
  private static final AtomicReference<ZooKeeperClientFactory> _zk = new AtomicReference<>();

  public static Path qualify(FileSystem fileSystem, Path path) {
    return path.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
  }

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
    String log4jConfigFile = getProperty(PACK_LOG4J_CONFIG);
    if (log4jConfigFile == null) {
      return;
    } else if (log4jConfigFile.endsWith(XML)) {
      DOMConfigurator.configure(log4jConfigFile);
    } else {
      PropertyConfigurator.configure(log4jConfigFile);
    }
  }

  public synchronized static UserGroupInformation getUserGroupInformation() throws IOException {
    UserGroupInformation userGroupInformation = UGI.get();
    if (userGroupInformation == null) {
      UGI.set(userGroupInformation = createUserGroupInformation());
    }
    userGroupInformation.checkTGTAndReloginFromKeytab();
    return userGroupInformation;
  }

  private static UserGroupInformation createUserGroupInformation() throws IOException {
    String hdfsPrinciaplName = getHdfsPrincipalName();
    LOGGER.info("hdfsPrinciaplName {}", hdfsPrinciaplName);
    String hdfsUser = getHdfsUser();
    LOGGER.info("hdfsUser {}", hdfsUser);
    if (hdfsPrinciaplName != null) {
      String hdfsKeytab = getHdfsKeytab();
      LOGGER.info("hdfsKeytab {}", hdfsKeytab);
      LOGGER.info("principal {} keytab location {}", hdfsPrinciaplName, hdfsKeytab);
      UserGroupInformation.loginUserFromKeytab(hdfsPrinciaplName, hdfsKeytab);
      return UserGroupInformation.getLoginUser();
    } else if (hdfsUser == null) {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      LOGGER.info("Using current user {}", ugi);
      return ugi;
    } else {
      return UserGroupInformation.createRemoteUser(hdfsUser);
    }
  }

  public static long getReloginInterval() {
    String v = getProperty(PACK_HDFS_KERBEROS_RELOGIN_INTERVAL);
    if (v == null) {
      return TimeUnit.SECONDS.toMinutes(10);
    }
    return Long.parseLong(v);
  }

  public static String getHdfsPrincipalName() {
    String v = getProperty(PACK_HDFS_KERBEROS_PRINCIPAL_NAME);
    if (v == null) {
      return null;
    }
    return v;
  }

  public static String getHdfsKeytab() {
    String v = getProperty(PACK_HDFS_KERBEROS_KEYTAB);
    if (v == null) {
      throw new RuntimeException("Keytab path not configured [" + PACK_HDFS_KERBEROS_KEYTAB + "].");
    }
    return v;
  }

  public static String getHdfsUser() {
    String v = getProperty(PACK_HDFS_USER);
    if (v == null) {
      return null;
    }
    return v;
  }

  public static void loadConfigIfExists(Configuration configuration, String dirPath) throws IOException {
    if (dirPath != null) {
      File dir = new File(dirPath);
      File core = new File(dir, CORE_SITE_XML);
      if (core.exists()) {
        configuration.addResource(new FileInputStream(core));
      }
      File hdfs = new File(dir, HDFS_SITE_XML);
      if (hdfs.exists()) {
        configuration.addResource(new FileInputStream(hdfs));
      }
    }
  }

  public static int getNumberOfMountSnapshots() {
    String v = getProperty(PACK_NUMBER_OF_MOUNT_SNAPSHOTS);
    if (v == null) {
      return PACK_NUMBER_OF_MOUNT_SNAPSHOTS_DEFAULT;
    }
    return Integer.parseInt(v);
  }

  public static long getVolumeMissingPollingPeriod() {
    String v = getProperty(PACK_VOLUME_MISSING_POLLING_PERIOD);
    if (v == null) {
      return PACK_VOLUME_MISSING_POLLING_PERIOD_DEFAULT;
    }
    return Long.parseLong(v);
  }

  public static int getVolumeMissingCountBeforeAutoShutdown() {
    String v = getProperty(PACK_VOLUME_MISSING_COUNT_BEFORE_AUTO_SHUTDOWN);
    if (v == null) {
      return PACK_VOLUME_MISSING_COUNT_BEFORE_AUTO_SHUTDOWN_DEFAULT;
    }
    return Integer.parseInt(v);
  }

  public static boolean getCountDockerDownAsMissing() {
    String v = getProperty(PACK_COUNT_DOCKER_DOWN_AS_MISSING);
    if (v == null) {
      return PACK_COUNT_DOCKER_DOWN_AS_MISSING_DEFAULT;
    }
    return Boolean.parseBoolean(v);
  }

  public static String getHdfsPath() {
    String v = getProperty(PACK_HDFS_PATH);
    if (v == null) {
      throw new RuntimeException("Hdfs path not configured [" + PACK_HDFS_PATH + "].");
    }
    return v;
  }

  private static String getProperty(String name) {
    return System.getenv(name);
  }

  public static boolean getNohupProcess() {
    String v = getProperty(PACK_NOHUP_PROCESS);
    if (v == null) {
      return true;
    }
    return Boolean.parseBoolean(v.toLowerCase());
  }

  public static String getLocalWorkingPath() {
    String v = getProperty(PACK_LOCAL);
    if (v == null) {
      return VAR_LIB_PACK;
    }
    return v;
  }

  public static boolean getFileSystemMount() {
    String v = getProperty(PACK_FILE_SYSTEM_MOUNT);
    if (v == null) {
      return false;
    }
    return Boolean.parseBoolean(v.toLowerCase());
  }

  public static String getLocalLogPath() {
    String v = getProperty(PACK_LOG);
    if (v == null) {
      return VAR_LOG_PACK;
    }
    return v;
  }

  // private static final AtomicReference<ZooKeeperClient> _zk = new
  // AtomicReference<ZooKeeperClient>();

  // public synchronized static ZooKeeperClient getZooKeeperClient() throws
  // IOException {
  // ZooKeeperClient zk = _zk.get();
  // if (zk == null) {
  // zk = ZkUtils.newZooKeeper(getZooKeeperConnectionString(),
  // getZooKeeperConnectionTimeout());
  // Runtime.getRuntime()
  // .addShutdownHook(new Thread(() -> Utils.close(LOGGER, _zk.get())));
  // _zk.set(zk);
  // }
  // return zk;
  // }

  public static String getZooKeeperConnectionString() {
    String v = getProperty(PACK_ZOOKEEPER_CONNECTION_STR);
    if (v == null) {
      throw new RuntimeException("ZooKeeper connection string not configured [" + PACK_ZOOKEEPER_CONNECTION_STR + "].");
    }
    return v;
  }

  public static int getZooKeeperConnectionTimeout() {
    String v = getProperty(PACK_ZOOKEEPER_CONNECTION_TIMEOUT);
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

  public static Result execAsResult(Logger logger, String... command) throws IOException {
    String uuid = UUID.randomUUID()
                      .toString();
    List<String> list = Arrays.asList(command);
    logger.info("Executing command id {} cmd {}", uuid, list);
    try {
      return PackServer.exec(uuid, list, logger);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      logger.info("Command id {} complete", uuid);
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

  public static void shutdownProcess(BlockPackFuse blockPackFuse) {
    if (blockPackFuse != null) {
      close(LOGGER, blockPackFuse);
    }
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    } catch (InterruptedException e) {
      LOGGER.info("Unknown error", e);
    }
    System.exit(0);
  }

  public static int getIntKey(long key) throws IOException {
    if (key < Integer.MAX_VALUE) {
      return (int) key;
    }
    throw new IOException("Key " + key + " is too large >= " + Integer.MAX_VALUE);
  }

  public static <T, E extends Throwable> T time(Logger logger, String name, TimerWithException<T, E> timerWithException)
      throws E {
    long start = System.nanoTime();
    try {
      return timerWithException.time();
    } finally {
      long end = System.nanoTime();
      LOGGER.info("Timer name {} took {} ms", name, (end - start) / 1_000_000.0);
    }
  }

  public static BytesWritable toBw(ByteBuffer byteBuffer) {
    ByteBuffer dup = byteBuffer.duplicate();
    byte[] buf = new byte[dup.remaining()];
    dup.get(buf);
    return new BytesWritable(buf);
  }

  public static File mkdir(File file) {
    file.mkdirs();
    return file;
  }

  public static void punchHole(Logger logger, File file, long offset, long length) throws IOException {
    Utils.exec(logger, FALLOCATE, KEEP_SIZE_SWITCH, PUNCH_HOLE_SWITCH, OFFSET_SWITCH, Long.toString(offset),
        LENGTH_SWTICH, Long.toString(length), file.getAbsolutePath());
  }

  public static long getNumberOfBlocksOnDisk(Logger logger, File file) throws IOException {
    Result result = Utils.execAsResult(logger, LS, ALLOCATED_SIZE_SWITCH, file.getAbsolutePath());
    if (result.exitCode == 0) {
      List<String> list = SPACE_SPLITTER.splitToList(result.stdout);
      return Long.parseLong(list.get(0));
    }
    throw new IOException("Error " + result.stderr);
  }

  public static <T> void shuffleArray(T[] ar) {
    Random rnd = ThreadLocalRandom.current();
    for (int i = ar.length - 1; i > 0; i--) {
      int index = rnd.nextInt(i + 1);
      T a = ar[index];
      ar[index] = ar[i];
      ar[i] = a;
    }
  }

  public static boolean isGlobalScope() {
    String v = getProperty(PACK_SCOPE);
    if (v != null && GLOBAL.equals(v.toLowerCase())) {
      return true;
    }
    return false;
  }

  public synchronized static ZooKeeperClientFactory getZooKeeperClientFactory() {
    ZooKeeperClientFactory zk = _zk.get();
    if (zk == null) {
      _zk.set(zk = ZkUtils.newZooKeeperClientFactory(getZooKeeperConnectionString(), getZooKeeperConnectionTimeout()));
    }
    return zk;
  }

  public static void crashJVM() throws Exception {
    Unsafe unsafe = getUnsafe();
    unsafe.putAddress(0, 0);
  }

  private static Unsafe getUnsafe() throws Exception {
    java.lang.reflect.Field singleoneInstanceField = Unsafe.class.getDeclaredField(THE_UNSAFE);
    singleoneInstanceField.setAccessible(true);
    return (Unsafe) singleoneInstanceField.get(null);
  }

}
