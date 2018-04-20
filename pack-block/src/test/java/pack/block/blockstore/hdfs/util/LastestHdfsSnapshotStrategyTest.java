package pack.block.blockstore.hdfs.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LastestHdfsSnapshotStrategyTest {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./target/tmp/LastestHdfsSnapshotStrategyTest");
  private static FileSystem fileSystem;

  @BeforeClass
  public static void beforeClass() throws IOException {
    Configuration configuration = new Configuration();
    String storePath = new File(storePathDir, "hdfs").getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();
  }

  @AfterClass
  public static void afterClass() {
    cluster.shutdown();
  }

  @Test
  public void testLastestHdfsSnapshotStrategy() throws Exception {
    Path path = new Path("/testLastestHdfsSnapshotStrategy/" + UUID.randomUUID()
                                                                   .toString());
    fileSystem.mkdirs(path);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    HdfsSnapshotUtil.enableSnapshots(fileSystem, path, ugi);
    int maxNumberOfMountSnapshots = 3;
    LastestHdfsSnapshotStrategy.setMaxNumberOfMountSnapshots(maxNumberOfMountSnapshots);
    LastestHdfsSnapshotStrategy strategy = new LastestHdfsSnapshotStrategy();

    List<String> list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String mountSnapshotName = HdfsSnapshotUtil.getMountSnapshotName(strategy);
      HdfsSnapshotUtil.createSnapshot(fileSystem, path, mountSnapshotName, ugi);
      list.add(mountSnapshotName);
      Thread.sleep(1000);
      HdfsSnapshotUtil.cleanupOldMountSnapshots(fileSystem, path, ugi, strategy);
    }
    System.out.println(list);
    FileStatus[] listStatus = fileSystem.listStatus(new Path(path, ".snapshot"));
    Arrays.sort(listStatus, Collections.reverseOrder());
    Collections.sort(list, Collections.reverseOrder());
    List<String> subList = new ArrayList<>(list.subList(0, maxNumberOfMountSnapshots));
    assertEquals(listStatus.length, subList.size());
    for (int i = 0; i < listStatus.length; i++) {
      FileStatus fileStatus = listStatus[i];
      assertEquals(fileStatus.getPath()
                             .getName(),
          subList.get(i));
    }
  }
}
