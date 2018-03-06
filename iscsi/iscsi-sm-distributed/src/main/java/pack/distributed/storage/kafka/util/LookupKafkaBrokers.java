package pack.distributed.storage.kafka.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.fasterxml.jackson.databind.ObjectMapper;

public class LookupKafkaBrokers {

  private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  private static final String SCHEME_ENDING = "://";
  private static final String BROKERS_IDS = "/brokers/ids";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException, URISyntaxException {
    List<String> list = getAllBrokers(new File("kafka-client.conf"));
    System.out.println(list);
  }

  public static List<String> getAllBrokers(File propertiesFile) {
    Properties properties = new Properties();
    try (InputStream inputStream = new FileInputStream(propertiesFile)) {
      properties.load(inputStream);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    String zkConnection = (String) properties.get(ZOOKEEPER_CONNECT);
    if (zkConnection == null) {
      throw new RuntimeException("Host missing kafka configuration from " + propertiesFile + ".");
    }

    return getAllBrokers(zkConnection);
  }

  public static List<String> getAllBrokers(String zkConnection) {
    try {
      ZooKeeper client = new ZooKeeper(zkConnection, 5000, event -> {
      });
      try {
        if (client.exists(BROKERS_IDS, false) == null) {
          return null;
        }
        List<String> list = client.getChildren(BROKERS_IDS, false);
        List<String> result = new ArrayList<>();
        for (String p : list) {
          String path = BROKERS_IDS + "/" + p;
          Stat stat = client.exists(path, false);
          if (stat != null) {
            byte[] data = client.getData(path, false, stat);
            KafkaBrokerEntry brokerEntry = OBJECT_MAPPER.readValue(data, KafkaBrokerEntry.class);
            List<String> endpoints = brokerEntry.getEndpoints();
            for (String endPoint : endpoints) {
              String hostAndPort = getHostAndPort(endPoint);
              result.add(hostAndPort);
            }
          }
        }
        return result;
      } finally {
        client.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String getHostAndPort(String endPoint) {
    int indexOf = endPoint.indexOf(SCHEME_ENDING);
    return endPoint.substring(indexOf + SCHEME_ENDING.length());
  }

}
