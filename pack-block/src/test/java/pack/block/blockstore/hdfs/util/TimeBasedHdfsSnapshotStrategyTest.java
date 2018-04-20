package pack.block.blockstore.hdfs.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimeBasedHdfsSnapshotStrategyTest {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./target/tmp/TimeBasedHdfsSnapshotStrategyTest");
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
  public void testTimeBasedHdfsSnapshotStrategy() throws Exception {
    Path path = new Path("/testTimeBasedHdfsSnapshotStrategy/" + UUID.randomUUID()
                                                                     .toString());
    fileSystem.mkdirs(path);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    HdfsSnapshotUtil.enableSnapshots(fileSystem, path, ugi);
    TimeBasedHdfsSnapshotStrategy strategy = new TimeBasedHdfsSnapshotStrategy();

    long now = System.currentTimeMillis();
    List<String> list = new ArrayList<>();
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.HOURS.toMillis(0)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.HOURS.toMillis(1)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.HOURS.toMillis(2)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.HOURS.toMillis(3)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.HOURS.toMillis(4)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.DAYS.toMillis(1)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.DAYS.toMillis(2)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.DAYS.toMillis(3)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.DAYS.toMillis(4)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.DAYS.toMillis(5)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.DAYS.toMillis(6)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.DAYS.toMillis(7)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.DAYS.toMillis(7 * 2)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.DAYS.toMillis(7 * 3)));
    list.add(HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.DAYS.toMillis(7 * 4)));

    for (int i = 0; i < 1000; i++) {
      String mountSnapshotName = HdfsSnapshotUtil.getMountSnapshotName(strategy, now - TimeUnit.HOURS.toMillis(i));
      HdfsSnapshotUtil.createSnapshot(fileSystem, path, mountSnapshotName, ugi);

      HdfsSnapshotUtil.cleanupOldMountSnapshots(fileSystem, path, ugi, strategy);
    }
    FileStatus[] listStatus = fileSystem.listStatus(new Path(path, ".snapshot"));
    Arrays.sort(listStatus, Collections.reverseOrder());
    Collections.sort(list, Collections.reverseOrder());
    for (int i = 0; i < listStatus.length; i++) {
      FileStatus fileStatus = listStatus[i];
      assertEquals(fileStatus.getPath()
                             .getName(),
          list.get(i));
    }
  }
}
