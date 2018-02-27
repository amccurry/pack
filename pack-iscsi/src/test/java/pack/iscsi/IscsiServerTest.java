package pack.iscsi;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import pack.iscsi.kafka.EmbeddedHdfsCluster;

public class IscsiServerTest {
  
  private static final String HADOOP_CONFIG_PATH = "HADOOP_CONFIG_PATH";
  private static EmbeddedHdfsCluster _embeddedHdfsCluster;
  private static ExecutorService _executorService;

  @BeforeClass
  public static void setup() throws IOException {
    if (runEmbeddedTests()) {
      _embeddedHdfsCluster = new EmbeddedHdfsCluster();
      _embeddedHdfsCluster.startup();
    }
    _executorService = Executors.newCachedThreadPool();
  }

  @AfterClass
  public static void teardown() {
    if (runEmbeddedTests()) {
      if (_embeddedHdfsCluster != null) {
        _embeddedHdfsCluster.shutdown();
      }
    }
    _executorService.shutdownNow();
  }
  
  
  private static boolean runEmbeddedTests() {
    return System.getenv(HADOOP_CONFIG_PATH) == null;
  }
}
