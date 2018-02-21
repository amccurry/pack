package pack.block.blockstore.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

public class HdfsMiniClusterProto {

  public static void main(String[] args) throws IOException, InterruptedException {
    File storePathDir = new File("./test");
    Configuration configuration = new Configuration();
    String storePath = storePathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
    builder.numDataNodes(0);
    MiniDFSCluster cluster = builder.build();

    String dataDir = "./testdatadir";
    startDatanode(configuration, cluster, dataDir);

    Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    List<DataNode> dataNodes = cluster.getDataNodes();
    for (DataNode dataNode : dataNodes) {
      System.out.println(dataNode);
    }
    cluster.shutdown();
  }

  private static void startDatanode(Configuration configuration, MiniDFSCluster cluster, String dataDir)
      throws IOException {
    String[] racks = new String[] { "/default" };
    Configuration conf = new Configuration(configuration);
    conf.set("dfs.data.dir", dataDir);
    cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, racks);
  }

}
