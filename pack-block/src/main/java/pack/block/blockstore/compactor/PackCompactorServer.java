package pack.block.blockstore.compactor;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.io.Closer;

import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.lock.HdfsLock;
import pack.block.blockstore.hdfs.lock.LockLostAction;
import pack.block.util.Utils;

public class PackCompactorServer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackCompactorServer.class);

  private static final String COMPACTION_LOCK = "compaction-lock";
  private static final String COMPACTION_THREAD = "compaction-thread";
  private static final String CONVERTER_THREAD = "converter-thread";
  private static final String CACHE = "compaction-cache";

  public static void main(String[] args) throws IOException, InterruptedException {
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

    try (PackCompactorServer packCompactorServer = new PackCompactorServer(cacheDir, configuration, pathList)) {
      Thread compactionThread = new Thread(() -> runCompaction(running, packCompactorServer));
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

  private static void runCompaction(AtomicBoolean running, PackCompactorServer packCompactorServer) {
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

  public PackCompactorServer(File cacheDir, Configuration configuration, List<Path> pathList) throws IOException {
    // coord with zookeeper use zookeeper to know if the block store is mount
    // (to know whether cleanup can be done)
    _cacheDir = cacheDir;
    _closer = Closer.create();
    _configuration = configuration;
    _pathList = pathList;
  }

  @Override
  public void close() throws IOException {
    _closer.close();
  }

  public void executeConverter() throws IOException, InterruptedException {
    for (Path path : _pathList) {
      executeConverter(path);
    }
  }

  public void executeConverter(Path root) throws IOException, InterruptedException {
    FileSystem fileSystem = root.getFileSystem(_configuration);
    FileStatus[] listStatus = order(fileSystem.listStatus(root));
    for (FileStatus status : listStatus) {
      try {
        executeConverterVolume(status.getPath());
      } catch (Exception e) {
        LOGGER.error("Convertions of " + status.getPath() + " failed", e);
      }
    }
  }

  private FileStatus[] order(FileStatus[] listStatus) {
    // Utils.shuffleArray(listStatus);
    Arrays.sort(listStatus);
    return listStatus;
  }

  private void executeConverterVolume(Path volumePath) throws IOException, InterruptedException {
    FileSystem fileSystem = volumePath.getFileSystem(_configuration);
    HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(fileSystem, volumePath);
    if (metaData == null) {
      Utils.dropVolume(volumePath, fileSystem);
      return;
    }

    try (WalToBlockFileConverter converter = new WalToBlockFileConverter(_cacheDir, fileSystem, volumePath, metaData,
        true)) {
      converter.runConverter();
    }
  }

  public void executeCompaction() throws IOException, InterruptedException {
    for (Path path : _pathList) {
      executeCompaction(path);
    }
  }

  public void executeCompaction(Path root) throws IOException, InterruptedException {
    FileSystem fileSystem = root.getFileSystem(_configuration);
    FileStatus[] listStatus = order(fileSystem.listStatus(root));
    for (FileStatus status : listStatus) {
      try {
        executeCompactionVolume(_configuration, status.getPath());
      } catch (Exception e) {
        LOGGER.error("Compaction of " + status.getPath() + " failed", e);
      }
    }
  }

  public static void executeCompactionVolume(Configuration configuration, Path volumePath)
      throws IOException, InterruptedException {
    FileSystem fileSystem = volumePath.getFileSystem(configuration);
    HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(fileSystem, volumePath);
    if (metaData == null) {
      Utils.dropVolume(volumePath, fileSystem);
      return;
    }
    Path lockPath = Utils.getLockPathForVolume(volumePath, COMPACTION_LOCK);
    LockLostAction lockLostAction = () -> {
      LOGGER.error("Compaction lock lost for volume {}", volumePath);
    };
    try (HdfsLock lock = new HdfsLock(configuration, lockPath, lockLostAction)) {
      if (lock.tryToLock()) {
        try (BlockFileCompactor compactor = new BlockFileCompactor(fileSystem, volumePath, metaData)) {
          compactor.runCompaction(lock);
        }
      } else {
        LOGGER.info("Skipping compaction no lock {}", volumePath);
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
