package pack.block.blockstore.compactor;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

import pack.zk.utils.ZkUtils;
import pack.zk.utils.ZooKeeperClient;

public class BlockFileCompactorServerUsing {

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

    Configuration configuration = new Configuration(true);
    configuration.addResource(new FileInputStream("./tmp-conf/hdfs-site.xml"));

    FileSystem fileSystem = FileSystem.get(configuration);

    List<Path> pathList = Arrays.asList(new Path("/block"));
    int sessionTimeout = 30000;
    try (ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper("localhost/localtestpack", sessionTimeout)) {
      try (PackCompactorServer packCompactorServer = new PackCompactorServer(fileSystem, pathList, zooKeeper)) {
        while (true) {
          packCompactorServer.executeCompaction();
          Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        }
      }
    }
  }

}
