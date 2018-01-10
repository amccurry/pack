package pack.block.blockstore.compactor;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.io.Closer;

import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.server.BlockPackFuse;
import pack.block.util.Utils;
import pack.zk.utils.ZkUtils;
import pack.zk.utils.ZooKeeperClient;
import pack.zk.utils.ZooKeeperLockManager;

public class PackCompactorServer implements Closeable {

  private static final String CACHE = "compaction-cache";

  private static final Logger LOGGER = LoggerFactory.getLogger(PackCompactorServer.class);

  private static final String COMPACTION = "/compaction";

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    Utils.setupLog4j();
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    String zkConnectionString = Utils.getZooKeeperConnectionString();
    int sessionTimeout = Utils.getZooKeeperConnectionTimeout();
    String hdfsPath = Utils.getHdfsPath();
    String localWorkingPath = Utils.getLocalWorkingPath();
    File cacheDir = new File(localWorkingPath, CACHE);
    cacheDir.mkdirs();
    AtomicBoolean running = new AtomicBoolean(true);
    ShutdownHookManager.get()
                       .addShutdownHook(() -> running.set(false), Integer.MAX_VALUE);

    Configuration configuration = new Configuration();
    FileSystem fileSystem = FileSystem.get(configuration);

    List<Path> pathList = getPathList(fileSystem, hdfsPath);

    try (PackCompactorServer packCompactorServer = new PackCompactorServer(cacheDir, fileSystem, pathList,
        zkConnectionString, sessionTimeout)) {
      while (running.get()) {
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
  private final Closer _closer;
  private final ZooKeeperLockManager _compactionLockManager;
  private final ZooKeeperLockManager _mountLockManager;
  private final File _cacheDir;
  private final String _zkConnectionString;
  private final int _sessionTimeout;

  public PackCompactorServer(File cacheDir, FileSystem fileSystem, List<Path> pathList, String zkConnectionString,
      int sessionTimeout) throws IOException {
    // coord with zookeeper
    // use zookeeper to know if the block store is mount (to know whether
    // cleanup can be done)
    _zkConnectionString = zkConnectionString;
    _sessionTimeout = sessionTimeout;
    _cacheDir = cacheDir;
    _closer = Closer.create();
    _fileSystem = fileSystem;
    _pathList = pathList;
    try (ZooKeeperClient zooKeeper = getZk()) {
      ZkUtils.mkNodesStr(zooKeeper, COMPACTION + "/lock");
    }
    _mountLockManager = BlockPackFuse.createLockmanager(_zkConnectionString, _sessionTimeout);
    _compactionLockManager = new ZooKeeperLockManager(_zkConnectionString, _sessionTimeout, COMPACTION + "/lock");
  }

  private ZooKeeperClient getZk() {
    try {
      return ZkUtils.newZooKeeper(_zkConnectionString, _sessionTimeout);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

        try (BlockFileCompactor compactor = new BlockFileCompactor(_cacheDir, _fileSystem, volumePath, metaData,
            _mountLockManager)) {
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
