package pack.iscsi.storage.utils;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.jmx.HierarchyDynamicMBean;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

public class PackUtils {

  private static final String ISCSI_PROPERTY_FILE = "ISCSI_PROPERTY_FILE";

  private static final String XML = ".xml";

  public static final String PACK_LOG4J_CONFIG = "PACK_LOG4J_CONFIG";

  private static final Logger LOGGER = LoggerFactory.getLogger(PackUtils.class);

  private static ThreadLocal<Random> _random = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  public static UUID generateSerialId() {
    return UUID.randomUUID();
  }

  public static void closeQuietly(Closeable... closeables) {
    if (closeables == null) {
      return;
    }
    for (Closeable closeable : closeables) {
      if (closeable != null) {
        try {
          closeable.close();
        } catch (IOException e) {

        }
      }
    }
  }
  
  public static void close(Logger logger, Object... objects) {
    if (objects == null) {
      return;
    }
    for (Object object : objects) {
      if (object != null && object instanceof Closeable) {
        Closeable closeable = (Closeable) object;
        try {
          closeable.close();
        } catch (IOException e) {
          logger.error("Unknown error", e);
        }
      }
    }
  }

  public static void close(Logger logger, Closeable... closeables) {
    if (closeables == null) {
      return;
    }
    for (Closeable closeable : closeables) {
      if (closeable != null) {
        try {
          closeable.close();
        } catch (IOException e) {
          logger.error("Unknown error", e);
        }
      }
    }
  }

  public static void close(Logger logger, ExecutorService... service) {
    close(logger, toCloseables(service));
  }

  private static Closeable[] toCloseables(ExecutorService[] service) {
    if (service == null) {
      return null;
    }
    Closeable[] closeables = new Closeable[service.length];
    int i = 0;
    for (ExecutorService executorService : service) {
      if (executorService != null) {
        closeables[i++] = () -> executorService.shutdownNow();
      }
    }
    return closeables;
  }

  public static void checkFutureIsRunning(Future<Void> future) {
    if (future == null) {
      return;
    }
    if (future.isDone()) {
      try {
        future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException("Future " + future + " failed");
      } catch (ExecutionException e) {
        Throwable throwable = e.getCause();
        throw (RuntimeException) throwable;
      }
    }
  }

  public static int getBlockOffset(long position, int blockSize) {
    return (int) (position % blockSize);
  }

  public static long getBlockId(long position, int blockSize) {
    return position / blockSize;
  }

  public static void assertIsValidForWriting(long storageIndex, int length, int blockSize) throws IOException {
    int blockOffset = getBlockOffset(storageIndex, blockSize);
    if (blockOffset != 0) {
      LOGGER.error("storage index {} is invalid produced blockOffset of {} with blockSize set to {}", storageIndex,
          blockOffset, blockSize);
      throw new IOException("storage index " + storageIndex + " is invalid produced blockOffset of " + blockOffset
          + " with blockSize set to " + blockSize);
    }
    if (length % blockSize != 0) {
      LOGGER.error("block length {} is invalid with blockSize set to {}", length, blockSize);
      throw new IOException("block length " + length + " is invalid with blockSize set to " + blockSize);
    }
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

  public static void closeQuietly(Future<?>... futures) {
    if (futures == null) {
      return;
    }
    for (Future<?> future : futures) {
      if (future != null) {
        future.cancel(true);
      }
    }
  }

  public static String getPropertyFailIfMissing(String name) {
    String value = getProperty(name, null);
    if (value == null) {
      throw new RuntimeException("required ENV/Property var " + name + " missing");
    }
    return value;
  }

  public static String getProperty(String name) {
    return getProperty(name, null);
  }

  public static String getProperty(String name, String defaultValue) {
    String value = System.getenv(name);
    if (value == null) {
      value = System.getProperty(name);
      if (value == null) {
        return defaultValue;
      }
    }
    return value;
  }

  public static List<String> getPropertyList(String name) {
    String value = getProperty(name);
    if (value == null) {
      return null;
    }
    return Splitter.on(',')
                   .splitToList(value);
  }

  public static List<String> getPropertyListFailIfMissing(String name) {
    List<String> list = getPropertyList(name);
    if (list == null) {
      throw new RuntimeException("required ENV/Property var " + name + " missing");
    }
    return list;
  }

  public static boolean isPropertySet(String name) {
    String env = System.getenv(name);
    if (env == null) {
      env = System.getProperty(name);
    }
    return env != null;
  }

  public static String getTopic(String name, String id) {
    return "pack." + name + "." + id;
  }

  public static long getPosition(int blockId, int blockSize) {
    return (long) blockId * (long) blockSize;
  }

  public static String toMd5(byte[] value) {
    try {
      return new BigInteger(MessageDigest.getInstance("md5")
                                         .digest(value)).toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] copy(byte[] bytes, int off, int len) {
    byte[] buf = new byte[len];
    System.arraycopy(bytes, off, buf, 0, len);
    return buf;
  }

  public static byte[] trimIfNeeded(byte[] bytes, int off, int len) {
    if (off == 0 && bytes.length == len) {
      return bytes;
    }
    byte[] buf = new byte[len];
    System.arraycopy(bytes, off, buf, 0, len);
    return buf;
  }

  public static void copy(InputStream input, OutputStream output) throws IOException {
    byte[] buf = new byte[1024];
    int num;
    while ((num = input.read(buf)) != -1) {
      output.write(buf, 0, num);
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
    try {
      registerLog4jMBeans();
    } catch (JMException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Register the log4j JMX mbeans. Set environment variable
   * "pack.jmx.log4j.disable" to true to disable registration.
   */
  public static void registerLog4jMBeans() throws JMException {
    if (Boolean.getBoolean("pack.jmx.log4j.disable") == true) {
      return;
    }

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    // Create and Register the top level Log4J MBean
    org.apache.log4j.jmx.HierarchyDynamicMBean hdm = new HierarchyDynamicMBean();

    ObjectName mbo = new ObjectName("log4j:hiearchy=default");
    mbs.registerMBean(hdm, mbo);

    // Add the root logger to the Hierarchy MBean
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
    hdm.addLoggerMBean(rootLogger.getName());

    // Get each logger from the Log4J Repository and add it to
    // the Hierarchy MBean created above.
    org.apache.log4j.spi.LoggerRepository r = org.apache.log4j.LogManager.getLoggerRepository();
    Enumeration<?> enumer = r.getCurrentLoggers();
    org.apache.log4j.Logger logger = null;

    while (enumer.hasMoreElements()) {
      logger = (org.apache.log4j.Logger) enumer.nextElement();
      hdm.addLoggerMBean(logger.getName());
    }

  }

  public static String getMapName(String name) {
    return "pack." + name;
  }

  public static long getRandomLong() {
    return getRandom().nextLong();
  }

  public static int getRandomInt() {
    return getRandom().nextInt();
  }

  public static void putInt(byte[] b, int off, int val) {
    b[off + 3] = (byte) (val);
    b[off + 2] = (byte) (val >>> 8);
    b[off + 1] = (byte) (val >>> 16);
    b[off] = (byte) (val >>> 24);
  }

  public static void putLong(byte[] b, int off, long val) {
    b[off + 7] = (byte) (val);
    b[off + 6] = (byte) (val >>> 8);
    b[off + 5] = (byte) (val >>> 16);
    b[off + 4] = (byte) (val >>> 24);
    b[off + 3] = (byte) (val >>> 32);
    b[off + 2] = (byte) (val >>> 40);
    b[off + 1] = (byte) (val >>> 48);
    b[off] = (byte) (val >>> 56);
  }

  public static int getInt(byte[] b, int off) {
    return ((b[off + 3] & 0xFF)) + ((b[off + 2] & 0xFF) << 8) + ((b[off + 1] & 0xFF) << 16) + ((b[off]) << 24);
  }

  public static long getLong(byte[] b, int off) {
    return ((b[off + 7] & 0xFFL)) + ((b[off + 6] & 0xFFL) << 8) + ((b[off + 5] & 0xFFL) << 16)
        + ((b[off + 4] & 0xFFL) << 24) + ((b[off + 3] & 0xFFL) << 32) + ((b[off + 2] & 0xFFL) << 40)
        + ((b[off + 1] & 0xFFL) << 48) + (((long) b[off]) << 56);
  }

  public static void closeOnShutdown(Closeable... closeables) {
    Runtime.getRuntime()
           .addShutdownHook(new Thread(() -> PackUtils.closeQuietly(closeables)));
  }

  public static void bootStrapProtpertyFile() throws IOException {
    String file = getProperty(ISCSI_PROPERTY_FILE);
    if (file == null) {
      return;
    }
    try (InputStream input = new FileInputStream(new File(file))) {
      System.getProperties()
            .load(input);
    }
  }

  public static int getAvailablePort() {
    try {
      ServerSocket socket = new ServerSocket(0);
      try {
        return socket.getLocalPort();
      } finally {
        socket.close();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Cannot find available port: " + e.getMessage(), e);
    }
  }

  public static File constructTempDir(String dirPrefix) {
    File file = new File(System.getProperty("java.io.tmpdir"), dirPrefix + getRandom().nextInt(10000000));
    if (!file.mkdirs()) {
      throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
    }
    file.deleteOnExit();
    return file;
  }

  public static Random getRandom() {
    return _random.get();
  }

  public static void write(DataOutput output, String s) throws IOException {
    byte[] bs = s.getBytes();
    output.writeInt(bs.length);
    output.write(bs);
  }

  public static String readString(DataInput input) throws IOException {
    int len = input.readInt();
    byte[] buf = new byte[len];
    input.readFully(buf);
    return new String(buf);
  }
}
