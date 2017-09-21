package pack.block.blockstore.compactor;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.io.Closer;

import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.server.BlockPackStorage;
import pack.block.util.Utils;
import pack.zk.utils.ZkUtils;
import pack.zk.utils.ZooKeeperClient;
import pack.zk.utils.ZooKeeperLockManager;

public class PackCompactorServer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackCompactorServer.class);

  private static final String COMPACTION = "/compaction";

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    Utils.setupLog4j();
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    String zkConnectionString = Utils.getZooKeeperConnectionString();
    int sessionTimeout = Utils.getZooKeeperConnectionTimeout();
    String hdfsPath = Utils.getHdfsPath();

    Configuration configuration = new Configuration();
    FileSystem fileSystem = FileSystem.get(configuration);

    ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper(zkConnectionString, sessionTimeout);
    List<Path> pathList = getPathList(fileSystem, hdfsPath);

    try (PackCompactorServer packCompactorServer = new PackCompactorServer(fileSystem, pathList, zooKeeper)) {
      while (true) {
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          try {
            packCompactorServer.executeCompaction();
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
          } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
          }
          return null;
        });
      }
    }
  }

  private final FileSystem _fileSystem;
  private final List<Path> _pathList;
  private final ZooKeeperLockManager _compactionLockManager;
  private final Closer _closer;
  private final ZooKeeperLockManager _mountLockManager;

  public PackCompactorServer(FileSystem fileSystem, List<Path> pathList, ZooKeeperClient zooKeeper) {
    // coord with zookeeper
    // use zookeeper to know if the block store is mount (to know whether
    // cleanup can be done)

    _closer = Closer.create();
    _fileSystem = fileSystem;
    _pathList = pathList;
    _closer.register(zooKeeper);
    ZkUtils.mkNodesStr(zooKeeper, COMPACTION + "/lock");
    _mountLockManager = BlockPackStorage.createLockmanager(zooKeeper);
    _compactionLockManager = new ZooKeeperLockManager(zooKeeper, COMPACTION + "/lock");
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
    String lockName = Utils.getLockName(volumePath);
    if (_compactionLockManager.tryToLock(lockName)) {
      try {
        HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(_fileSystem, volumePath);
        long maxBlockFileSize = metaData.getMaxBlockFileSize();
        double maxObsoleteRatio = metaData.getMaxObsoleteRatio();
        try (BlockFileCompactor compactor = new BlockFileCompactor(_fileSystem, volumePath, maxBlockFileSize,
            maxObsoleteRatio, _mountLockManager)) {
          compactor.runCompaction();
        }
      } finally {
        _compactionLockManager.unlock(lockName);
      }
    }
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

}
