package pack.iscsi.bk;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class TestProperties {

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

  public static String getZooKeeperConnection() {
    return PROPERTIES.getProperty("zk");
  }

  public static String getMetadataServiceUri() {
    return PROPERTIES.getProperty("metadataUri");
  }

}
