package pack.iscsi.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SimpleTest {

  private static EmbeddedZookeeper _embeddedZookeeper;
  private static EmbeddedKafkaCluster _embeddedKafkaCluster;
  private static EmbeddedHdfsCluster _embeddedHdfsCluster;

  @BeforeClass
  public static void setup() throws IOException {
    _embeddedZookeeper = new EmbeddedZookeeper();
    _embeddedZookeeper.startup();
    _embeddedHdfsCluster = new EmbeddedHdfsCluster();
    _embeddedHdfsCluster.startup();
    _embeddedKafkaCluster = new EmbeddedKafkaCluster(_embeddedZookeeper.getConnection(), new Properties(),
        Arrays.asList(-1, -1, -1));
    _embeddedKafkaCluster.startup();
  }

  @AfterClass
  public static void teardown() {
    if (_embeddedKafkaCluster != null) {
      _embeddedKafkaCluster.shutdown();
      _embeddedKafkaCluster.awaitShutdown();
    }
    if (_embeddedHdfsCluster != null) {
      _embeddedHdfsCluster.shutdown();
    }
    if (_embeddedZookeeper != null) {
      _embeddedZookeeper.shutdown();
    }
  }

  public void testSomething() throws InterruptedException, ExecutionException, IOException {
    String connection = _embeddedZookeeper.getConnection();
    System.out.println(connection);
    String brokerList = _embeddedKafkaCluster.getBrokerList();
    System.out.println(brokerList);
    FileSystem fileSystem = _embeddedHdfsCluster.getFileSystem();
    System.out.println(fileSystem);
  }
}
