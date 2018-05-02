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

  private static final Logger LOGGER = LoggerFactory.getLogger(PackCompactorServer.class);

  private static final String COMPACTION_THREAD = "compaction-thread";
  private static final String CONVERTER_THREAD = "converter-thread";
  private static final String CACHE = "compaction-cache";
  private static final String COMPACTION = "/compaction";
  private static final String CONVERTER = "/converter";

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

      Thread compactionThread = new Thread(() -> runCompaction(ugi, running, packCompactorServer));
      compactionThread.setName(COMPACTION_THREAD);
      compactionThread.start();

      Thread converterThread = new Thread(() -> runConverter(ugi, running, packCompactorServer));
      converterThread.setName(CONVERTER_THREAD);
      converterThread.start();

      compactionThread.join();
      converterThread.join();
    }
  }

  private static void runConverter(UserGroupInformation ugi, AtomicBoolean running,
      PackCompactorServer packCompactorServer) {
    while (running.get()) {
      try {
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          try {
            packCompactorServer.executeConverter();
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
          } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
          }
          return null;
        });
      } catch (Exception e) {
        LOGGER.error("Unknown error", e);
      }
    }
  }

  private static void runCompaction(UserGroupInformation ugi, AtomicBoolean running,
      PackCompactorServer packCompactorServer) {
    while (running.get()) {
      try {
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          try {
            packCompactorServer.executeCompaction();
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
          } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
          }
          return null;
        });
      } catch (Exception e) {
        LOGGER.error("Unknown error", e);
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
  private final ZooKeeperLockManager _converterLockManager;

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
      ZkUtils.mkNodesStr(zooKeeper, CONVERTER + "/lock");
    }
    _mountLockManager = BlockPackFuse.createLockmanager(_zkConnectionString, _sessionTimeout);
    _compactionLockManager = new ZooKeeperLockManager(_zkConnectionString, _sessionTimeout, COMPACTION + "/lock");
    _converterLockManager = new ZooKeeperLockManager(_zkConnectionString, _sessionTimeout, CONVERTER + "/lock");
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

  public void executeConverter() throws IOException, KeeperException, InterruptedException {
    for (Path path : _pathList) {
      executeConverter(path);
    }
  }

  public void executeConverter(Path root) throws IOException, KeeperException, InterruptedException {
    FileStatus[] listStatus = _fileSystem.listStatus(root);
    for (FileStatus status : listStatus) {
      try {
        executeConverterVolume(status.getPath());
      } catch (Exception e) {
        LOGGER.error("Convertions of " + status.getPath() + " failed", e);
      }
    }
  }

  private void executeConverterVolume(Path volumePath) throws IOException, KeeperException, InterruptedException {
    HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(_fileSystem, volumePath);
    if (metaData == null) {
      return;
    }
    try (WalToBlockFileConverter converter = new WalToBlockFileConverter(_cacheDir, _fileSystem, volumePath, metaData,
        _converterLockManager)) {
      converter.runConverter();
    }
  }

  public void executeCompaction() throws IOException, KeeperException, InterruptedException {
    for (Path path : _pathList) {
      executeCompaction(path);
    }
  }

  public void executeCompaction(Path root) throws IOException, KeeperException, InterruptedException {
    FileStatus[] listStatus = _fileSystem.listStatus(root);
    for (FileStatus status : listStatus) {
      try {
        executeCompactionVolume(status.getPath());
      } catch (Exception e) {
        LOGGER.error("Compaction of " + status.getPath() + " failed", e);
      }
    }
  }

  private void executeCompactionVolume(Path volumePath) throws IOException, KeeperException, InterruptedException {
    String lockName = Utils.getLockName(volumePath);
    if (_compactionLockManager.tryToLock(lockName)) {
      try {
        HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(_fileSystem, volumePath);
        if (metaData == null) {
          return;
        }
        try (BlockFileCompactor compactor = new BlockFileCompactor(_fileSystem, volumePath, metaData,
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
