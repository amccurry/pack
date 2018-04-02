package pack.distributed.storage.minicluster;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;

import pack.iscsi.storage.utils.PackUtils;

public class IscsiMiniCluster {

  public static void main(String[] args) throws IOException {
    IscsiMiniCluster cluster = new IscsiMiniCluster();
    String zkConnection = cluster.getZkConnection();
    String kafkaZkConnection = cluster.getKafkaZkConnection();
    Configuration conf = cluster.getHdfsConfiguration();

    File confDir = new File("mini-cluster-conf");
    PackUtils.rmr(confDir);
    confDir.mkdirs();
    File hadoopConf = new File(confDir, "hadoop-conf");
    hadoopConf.mkdirs();
    writeConfig(conf, hadoopConf);

    String address = "192.168.56.1";
    try (PrintWriter writer = new PrintWriter(new File(confDir, "minicluster.env"))) {
      writer.printf("export %s=%s%n", "HDFS_CONF_PATH", hadoopConf.getAbsolutePath());
      writer.printf("export %s=%s%n", "HDFS_TARGET_PATH", "/pack");
      writer.printf("export %s=%s%n", "KAFKA_ZK_CONNECTION", kafkaZkConnection);
      writer.printf("export %s=%s%n", "WAL_CACHE_DIR", "./wal-dir");
      writer.printf("export %s=%s%n", "PACK_ISCSI_ADDRESS", address);
      writer.printf("export %s=%s%n", "ZK_CONNECTION", zkConnection);
    }

    File propFile = new File(confDir, "minicluster.property");
    try (PrintWriter writer = new PrintWriter(propFile)) {
      writer.printf("%s=%s%n", "HDFS_CONF_PATH", hadoopConf.getAbsolutePath());
      writer.printf("%s=%s%n", "HDFS_TARGET_PATH", "/pack");
      writer.printf("%s=%s%n", "KAFKA_ZK_CONNECTION", kafkaZkConnection);
      writer.printf("%s=%s%n", "WAL_CACHE_DIR", "./wal-dir");
      writer.printf("%s=%s%n", "PACK_ISCSI_ADDRESS", address);
      writer.printf("%s=%s%n", "ZK_CONNECTION", zkConnection);
    }
  }

  public static void writeConfig(Configuration conf, File hadoopConf) throws IOException {
    hadoopConf.mkdirs();
    try (FileOutputStream out = new FileOutputStream(new File(hadoopConf, "hdfs-site.xml"))) {
      conf.writeXml(out);
    }
    try (FileOutputStream out = new FileOutputStream(new File(hadoopConf, "core-site.xml"))) {
      conf.writeXml(out);
    }
  }

  private final EmbeddedKafkaCluster _embeddedKafkaCluster;
  private final EmbeddedZookeeper _embeddedZookeeper;
  private final EmbeddedHdfsCluster _embeddedHdfsCluster;

  public IscsiMiniCluster() throws IOException {
    _embeddedZookeeper = new EmbeddedZookeeper();
    _embeddedZookeeper.startup();
    String zkKafkaConnection = _embeddedZookeeper.getConnection() + "/kafka";
    _embeddedKafkaCluster = new EmbeddedKafkaCluster(zkKafkaConnection);
    _embeddedKafkaCluster.startup();
    _embeddedHdfsCluster = new EmbeddedHdfsCluster();
    _embeddedHdfsCluster.startup();
  }

  public Configuration getHdfsConfiguration() throws IOException {
    return _embeddedHdfsCluster.getFileSystem()
                               .getConf();
  }

  public String getZkConnection() {
    return _embeddedZookeeper.getConnection();
  }

  public String getKafkaZkConnection() {
    return _embeddedKafkaCluster.getZkConnection();
  }

}
