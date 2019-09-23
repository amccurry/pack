package pack.iscsi.wal;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class WalTestProperties {

  private static final Properties PROPERTIES;

  static {
    Properties properties = new Properties();
    try {
      File file = new File("test.properties");
      if (!file.exists()) {
        throw new RuntimeException("File " + file.getCanonicalPath() + " not found");
      }
      try (FileInputStream input = new FileInputStream(file)) {
        properties.load(input);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    PROPERTIES = properties;
  }

  public static String getPrefix() {
    return PROPERTIES.getProperty("zk.prefix.wal");
  }

  public static String getZooKeeperConnection() {
    return PROPERTIES.getProperty("zk");
  }

}