package pack.block.blockstore.compactor;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Splitter;
import com.google.common.io.Closer;

import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.zk.utils.ZkUtils;
import pack.zk.utils.ZooKeeperClient;
import pack.zk.utils.ZooKeeperLockManager;

public class PackCompactorServer implements Closeable {

  private static final String PACK_ROOT_PATH_LIST = "PACK_ROOT_PATH_LIST";
  private static final String COMPACTION = "/compaction";
  private static final String ZK_SESSION_TIMEOUT = "ZK_SESSION_TIMEOUT";
  private static final String ZK_CONECTION_STR = "ZK_CONECTION_STR";
  private static final String HDFS_CONFIG_DIR = "HDFS_CONFIG_DIR";

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    String dirPath = System.getenv(HDFS_CONFIG_DIR);
    String zkConnectionString = System.getenv(ZK_CONECTION_STR);
    int sessionTimeout = Integer.parseInt(System.getenv(ZK_SESSION_TIMEOUT));
    String packRootPathList = System.getenv(PACK_ROOT_PATH_LIST);

    Configuration configuration = new Configuration();
    loadConfigIfExists(configuration, dirPath);
    FileSystem fileSystem = FileSystem.get(configuration);

    ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper(zkConnectionString + COMPACTION, sessionTimeout);
    List<Path> pathList = getPathList(fileSystem, packRootPathList);

    try (PackCompactorServer packCompactorServer = new PackCompactorServer(fileSystem, pathList, zooKeeper)) {
      while (true) {
        packCompactorServer.executeCompaction();
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      }
    }
  }

  private final FileSystem _fileSystem;
  private final List<Path> _pathList;
  private final ZooKeeperLockManager _lockManager;
  private final Closer _closer;

  public PackCompactorServer(FileSystem fileSystem, List<Path> pathList, ZooKeeperClient zooKeeper) {
    // coord with zookeeper
    // use zookeeper to know if the block store is mount (to know whether
    // cleanup can be done)

    _closer = Closer.create();
    _fileSystem = fileSystem;
    _pathList = pathList;
    _closer.register(zooKeeper);
    ZkUtils.mkNodesStr(zooKeeper, "/lock");
    _lockManager = new ZooKeeperLockManager(zooKeeper, "/lock");
  }

  @Override
  public void close() throws IOException {
    _closer.close();
  }

  public void executeCompaction() throws IOException, KeeperException, InterruptedException {
    for (Path path : _pathList) {
      executeCompaction(path);
    }
  }

  public void executeCompaction(Path root) throws IOException, KeeperException, InterruptedException {
    FileStatus[] listStatus = _fileSystem.listStatus(root);
    for (FileStatus status : listStatus) {
      executeCompactionVolume(status.getPath());
    }
  }

  private void executeCompactionVolume(Path volumePath) throws IOException, KeeperException, InterruptedException {
    String lockName = getLockName(volumePath);
    if (_lockManager.tryToLock(lockName)) {
      try {
        HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(_fileSystem, volumePath);
        long maxBlockFileSize = metaData.getMaxBlockFileSize();
        try (BlockFileCompactor compactor = new BlockFileCompactor(_fileSystem, volumePath, maxBlockFileSize)) {
          compactor.runCompaction();
        }
      } finally {
        _lockManager.unlock(lockName);
      }
    }
  }

  private String getLockName(Path volumePath) {
    String path = volumePath.toUri()
                            .getPath();
    return path.replaceAll("/", "__");
  }

  private static List<Path> getPathList(FileSystem fileSystem, String packRootPathList) throws IOException {
    List<String> list = Splitter.on(',')
                                .splitToList(packRootPathList);
    List<Path> pathList = new ArrayList<>();
    for (String p : list) {
      FileStatus fileStatus = fileSystem.getFileStatus(new Path(p));
      pathList.add(fileStatus.getPath());
    }
    return pathList;
  }

  private static void loadConfigIfExists(Configuration configuration, String dirPath) throws FileNotFoundException {
    if (dirPath != null) {
      File dir = new File(dirPath);
      File core = new File(dir, "core-site.xml");
      if (core.exists()) {
        configuration.addResource(new FileInputStream(core));
      }
      File hdfs = new File(dir, "hdfs-site.xml");
      if (hdfs.exists()) {
        configuration.addResource(new FileInputStream(hdfs));
      }
    }
  }

}
