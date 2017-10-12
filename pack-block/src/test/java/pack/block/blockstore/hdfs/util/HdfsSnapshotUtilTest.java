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

public class HdfsSnapshotUtilTest {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./target/tmp/HdfsSnapshotUtilTest");
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
  public void testHdfsSnapshotUtil() throws Exception {
    Path path = new Path("/testHdfsSnapshotUtil/" + UUID.randomUUID()
                                                        .toString());
    fileSystem.mkdirs(path);
    int maxNumberOfMountSnapshots = 3;
    List<String> list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String mountSnapshotName = HdfsSnapshotUtil.getMountSnapshotName();
      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("hdfs",
          UserGroupInformation.getCurrentUser());
      HdfsSnapshotUtil.createSnapshot(fileSystem, path, mountSnapshotName, proxyUser);
      list.add(mountSnapshotName);
      Thread.sleep(1000);
      HdfsSnapshotUtil.cleanupOldMountSnapshots(fileSystem, path, maxNumberOfMountSnapshots, proxyUser);
    }
    System.out.println(list);
    FileStatus[] listStatus = fileSystem.listStatus(new Path(path, ".snapshot"));
    Arrays.sort(listStatus, Collections.reverseOrder());
    List<String> subList = new ArrayList<>(list.subList(list.size() - maxNumberOfMountSnapshots, list.size()));
    Collections.sort(subList, Collections.reverseOrder());
    assertEquals(listStatus.length, subList.size());
    for (int i = 0; i < listStatus.length; i++) {
      FileStatus fileStatus = listStatus[i];
      assertEquals(fileStatus.getPath()
                             .getName(),
          subList.get(i));
    }
  }
}
