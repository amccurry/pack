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
import pack.zk.utils.ZooKeeperClientFactory;
import pack.zk.utils.ZooKeeperLockManager;

public class PackCompactorServer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackCompactorServer.class);

  private static final String COMPACTION_THREAD = "compaction-thread";
  private static final String CONVERTER_THREAD = "converter-thread";
  private static final String CACHE = "compaction-cache";

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    Utils.setupLog4j();

    String hdfsPath = Utils.getHdfsPath();
    String localWorkingPath = Utils.getLocalWorkingPath();
    File cacheDir = new File(localWorkingPath, CACHE);
    cacheDir.mkdirs();
    AtomicBoolean running = new AtomicBoolean(true);
    ShutdownHookManager.get()
                       .addShutdownHook(() -> running.set(false), Integer.MAX_VALUE);

    Configuration configuration = new Configuration();
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    List<Path> pathList = ugi.doAs((PrivilegedExceptionAction<List<Path>>) () -> getPathList(configuration, hdfsPath));

    ZooKeeperClientFactory zk = Utils.getZooKeeperClientFactory();

    try (PackCompactorServer packCompactorServer = new PackCompactorServer(cacheDir, configuration, pathList, zk)) {
      Thread compactionThread = new Thread(() -> runCompaction(running, packCompactorServer, zk));
      compactionThread.setName(COMPACTION_THREAD);
      compactionThread.start();

      Thread converterThread = new Thread(() -> runConverter(running, packCompactorServer));
      converterThread.setName(CONVERTER_THREAD);
      converterThread.start();

      compactionThread.join();
      converterThread.join();
    }
  }

  private static void runConverter(AtomicBoolean running, PackCompactorServer packCompactorServer) {
    while (running.get()) {
      try {
        UserGroupInformation ugi = Utils.getUserGroupInformation();
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          try {
            packCompactorServer.executeConverter();
          } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
          }
          return null;
        });
      } catch (Exception e) {
        LOGGER.error("Unknown error", e);
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  private static void runCompaction(AtomicBoolean running, PackCompactorServer packCompactorServer,
      ZooKeeperClientFactory zk) {
    while (running.get()) {
      try {
        UserGroupInformation ugi = Utils.getUserGroupInformation();
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          try {
            packCompactorServer.executeCompaction();
          } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
          }
          return null;
        });
      } catch (Exception e) {
        LOGGER.error("Unknown error", e);
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  private final List<Path> _pathList;
  private final Closer _closer;
  private final File _cacheDir;
  private final Configuration _configuration;
  private final ZooKeeperClientFactory _zk;

  public PackCompactorServer(File cacheDir, Configuration configuration, List<Path> pathList, ZooKeeperClientFactory zk)
      throws IOException {
    // coord with zookeeper use zookeeper to know if the block store is mount
    // (to know whether cleanup can be done)
    _zk = zk;
    _cacheDir = cacheDir;
    _closer = Closer.create();
    _configuration = configuration;
    _pathList = pathList;
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
    FileSystem fileSystem = root.getFileSystem(_configuration);
    FileStatus[] listStatus = randomOrder(fileSystem.listStatus(root));
    for (FileStatus status : listStatus) {
      try {
        executeConverterVolume(status.getPath());
      } catch (Exception e) {
        LOGGER.error("Convertions of " + status.getPath() + " failed", e);
      }
    }
  }

  private FileStatus[] randomOrder(FileStatus[] listStatus) {
    Utils.shuffleArray(listStatus);
    return listStatus;
  }

  private void executeConverterVolume(Path volumePath) throws IOException, KeeperException, InterruptedException {
    FileSystem fileSystem = volumePath.getFileSystem(_configuration);
    HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(fileSystem, volumePath);
    if (metaData == null) {
      return;
    }
    try (ZooKeeperLockManager converterLockManager = WalToBlockFileConverter.createLockmanager(_zk,
        volumePath.getName())) {
      try (WalToBlockFileConverter converter = new WalToBlockFileConverter(_cacheDir, fileSystem, volumePath, metaData,
          converterLockManager)) {
        converter.runConverter();
      }
    }
  }

  public void executeCompaction() throws IOException, KeeperException, InterruptedException {
    for (Path path : _pathList) {
      executeCompaction(path);
    }
  }

  public void executeCompaction(Path root) throws IOException, KeeperException, InterruptedException {
    FileSystem fileSystem = root.getFileSystem(_configuration);
    FileStatus[] listStatus = randomOrder(fileSystem.listStatus(root));
    for (FileStatus status : listStatus) {
      try {
        executeCompactionVolume(status.getPath());
      } catch (Exception e) {
        LOGGER.error("Compaction of " + status.getPath() + " failed", e);
      }
    }
  }

  private void executeCompactionVolume(Path volumePath) throws IOException, KeeperException, InterruptedException {
    FileSystem fileSystem = volumePath.getFileSystem(_configuration);
    String lockName = Utils.getLockName(volumePath);
    try (ZooKeeperLockManager compactionLockManager = BlockFileCompactor.createLockmanager(_zk, volumePath.getName())) {
      if (compactionLockManager.tryToLock(lockName)) {
        try {
          HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(fileSystem, volumePath);
          if (metaData == null) {
            return;
          }
          try (ZooKeeperLockManager mountLockManager = BlockPackFuse.createLockmanager(_zk, volumePath.getName())) {
            try (BlockFileCompactor compactor = new BlockFileCompactor(fileSystem, volumePath, metaData,
                mountLockManager)) {
              compactor.runCompaction();
            }
          }
        } finally {
          compactionLockManager.unlock(lockName);
        }
      }
    }
  }

  private static List<Path> getPathList(Configuration configuration, String packRootPathList) throws IOException {
    List<String> list = Splitter.on(',')
                                .splitToList(packRootPathList);
    List<Path> pathList = new ArrayList<>();
    for (String p : list) {
      Path path = new Path(p);
      FileSystem fileSystem = path.getFileSystem(configuration);
      FileStatus fileStatus = fileSystem.getFileStatus(path);
      pathList.add(fileStatus.getPath());
    }
    return pathList;
  }

}
