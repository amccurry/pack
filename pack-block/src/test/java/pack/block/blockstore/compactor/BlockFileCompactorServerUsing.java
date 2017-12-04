package pack.block.blockstore.compactor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

public class BlockFileCompactorServerUsing {

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

    Configuration configuration = new Configuration(true);
    configuration.addResource(new FileInputStream("./tmp-conf/hdfs-site.xml"));

    FileSystem fileSystem = FileSystem.get(configuration);

    List<Path> pathList = Arrays.asList(new Path("/block"));
    int sessionTimeout = 30000;
    try (PackCompactorServer packCompactorServer = new PackCompactorServer(new File("./tmp"), fileSystem, pathList,
        "localhost/localtestpack", sessionTimeout)) {
      while (true) {
        packCompactorServer.executeCompaction();
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      }
    }
  }

}
