package pack.iscsi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.iscsi.kafka.EmbeddedHdfsCluster;
import pack.iscsi.kafka.EmbeddedKafkaCluster;
import pack.iscsi.kafka.EmbeddedZookeeper;

public class IscsiServerEmbedded {

  private static final String XML = ".xml";

  private static final String HADOOP_CONFIG_PATH = "HADOOP_CONFIG_PATH";
  private static final String KAFKA_BROKER_LIST = "KAFKA_BROKER_LIST";

  private static EmbeddedHdfsCluster _embeddedHdfsCluster;
  private static EmbeddedZookeeper _embeddedZookeeper;
  private static EmbeddedKafkaCluster _embeddedKafkaCluster;

  public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
    startupServers();

    ImmutableSet<String> addresses = ImmutableSet.of("192.168.56.1");
    List<String> brokerServers = Splitter.on(',')
                                         .splitToList(getBrokerList());
    File cacheDir = new File("./target/IscsiServerEmbedded/cache");
    Configuration configuration = getConfiguration();
    HdfsBlockStoreConfig hdfsBlockStoreConfig = HdfsBlockStoreConfig.DEFAULT_CONFIG;
    TargetManager iscsiTargetManager = new BaseTargetManager("2018-03", "test");
    int port = 3260;
    Path root = new Path("/tmp/pack");
    String serialId = "1234567890";
    IscsiServerConfig config = IscsiServerConfig.builder()
                                                .addresses(addresses)
                                                .brokerServers(brokerServers)
                                                .cacheDir(cacheDir)
                                                .configuration(configuration)
                                                .hdfsBlockStoreConfig(hdfsBlockStoreConfig)
                                                .hdfsStorageModuleEnabled(false)
                                                .iscsiTargetManager(iscsiTargetManager)
                                                .port(port)
                                                .root(root)
                                                .serialId(serialId)
                                                .ugi(UserGroupInformation.getCurrentUser())
                                                .build();
    FileSystem fileSystem = root.getFileSystem(configuration);
    Path volumePath = new Path(root, "test1");
    fileSystem.delete(volumePath, true);
    fileSystem.mkdirs(volumePath);
    deleteTopic("test1", getBrokerList());
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .fileSystemBlockSize(512)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, volumePath);
    Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    try (IscsiServer server = new IscsiServer(config)) {
      server.registerTargets();
      server.start();
      server.join();
    }

    shutdownServers();
  }

  private static void shutdownServers() {
    if (runHdfsEmbeddedTests()) {
      if (_embeddedHdfsCluster != null) {
        _embeddedHdfsCluster.shutdown();
      }
    }
    if (runKafkaEmbeddedTests()) {
      if (_embeddedKafkaCluster != null) {
        _embeddedKafkaCluster.shutdown();
      }
      if (_embeddedZookeeper != null) {
        _embeddedZookeeper.shutdown();
      }
    }
  }

  private static void startupServers() throws IOException {
    if (runKafkaEmbeddedTests()) {
      _embeddedZookeeper = new EmbeddedZookeeper();
      _embeddedZookeeper.startup();
      _embeddedKafkaCluster = new EmbeddedKafkaCluster(_embeddedZookeeper.getConnection(), new Properties(),
          Arrays.asList(-1, -1, -1));
      _embeddedKafkaCluster.startup();
    }
    if (runHdfsEmbeddedTests()) {
      _embeddedHdfsCluster = new EmbeddedHdfsCluster();
      _embeddedHdfsCluster.startup();
    }
  }

  private static boolean runKafkaEmbeddedTests() {
    return System.getenv(KAFKA_BROKER_LIST) == null;
  }

  private static boolean runHdfsEmbeddedTests() {
    return System.getenv(HADOOP_CONFIG_PATH) == null;
  }

  private static Configuration getConfiguration() throws IOException {
    if (runHdfsEmbeddedTests()) {
      return _embeddedHdfsCluster.getFileSystem()
                                 .getConf();
    } else {
      Configuration configuration = new Configuration();
      String path = System.getenv(HADOOP_CONFIG_PATH);
      File file = new File(path);
      if (file.exists() && file.isDirectory()) {
        for (File f : file.listFiles((FilenameFilter) (dir, name) -> name.endsWith(XML))) {
          configuration.addResource(new FileInputStream(f));
        }
      }
      return configuration;
    }
  }

  private static String getBrokerList() {
    if (runKafkaEmbeddedTests()) {
      return _embeddedKafkaCluster.getBrokerList();
    } else {
      return System.getenv(KAFKA_BROKER_LIST);
    }
  }

  public static void deleteTopic(String kafkaTopic, String bootstrapServers)
      throws InterruptedException, ExecutionException {
    Properties props = new Properties();
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    try (AdminClient adminClient = AdminClient.create(props)) {
      Set<String> names = adminClient.listTopics()
                                     .names()
                                     .get();
      if (names.contains(kafkaTopic)) {
        adminClient.deleteTopics(ImmutableList.of(kafkaTopic));
      }
    }
  }
}
