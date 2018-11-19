package pack.block.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Closer;

import pack.block.blockstore.BlockStore;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImplConfig;
import pack.block.blockstore.hdfs.file.ImmutableRoaringBitmapManager;
import pack.block.blockstore.hdfs.lock.LockLostAction;
import pack.block.blockstore.hdfs.lock.PackLock;
import pack.block.blockstore.hdfs.lock.PackLockFactory;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import pack.block.blockstore.hdfs.util.HdfsSnapshotUtil;
import pack.block.blockstore.hdfs.util.LastestHdfsSnapshotStrategy;
import pack.block.fuse.FuseFileSystemSingleMount;
import pack.block.fuse.SnapshotHandler;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.server.admin.Status;
import pack.block.server.json.BlockPackFuseConfig;
import pack.block.server.json.BlockPackFuseConfigInternal;
import pack.block.server.json.BlockPackFuseConfigInternal.BlockPackFuseConfigInternalBuilder;
import pack.block.util.Utils;
import pack.util.ExecUtil;
import pack.util.Result;

public class BlockPackFuse implements Closeable {

  private static final String LOG = ".log";
  private static final String YYYY_MM_DD_HH_MM_SS = "yyyy.MM.dd-HH.mm.ss";
  private static final boolean ENABLE_HDFS_LOGS = false;

  private static <T extends Closeable> T autoClose(T t, int priority) {
    ShutdownHookManager.get()
                       .addShutdownHook(() -> {
                         IOUtils.closeQuietly(t);
                       }, priority);
    return t;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackFuse.class);
  private static final Logger STATUS_LOGGER = LoggerFactory.getLogger("STATUS");

  private static final String MOUNT = "mount";
  private static final String SUDO = "sudo";
  private static final String UMOUNT = "umount";

  private static final String FUSE_MOUNT_THREAD = "fuse-mount-thread";
  private static final String SITE_XML = "-site.xml";
  private static final String HDFS_CONF = "hdfs-conf";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int AUTO_UMOUNT_PRIORITY = 10000;
  private static final int FUSE_CLOSE_PRIORITY = 9000;

  public static void main(String[] args) throws Exception {
    try {
      Utils.setupLog4j();
      LOGGER.info("log4j setup");
      Configuration conf = getConfig();
      LOGGER.info("hdfs config read");
      UserGroupInformation ugi = Utils.getUserGroupInformation();
      LOGGER.info("hdfs ugi created");

      BlockPackFuseConfig blockPackFuseConfig = MAPPER.readValue(new File(args[0]), BlockPackFuseConfig.class);
      Path path = new Path(blockPackFuseConfig.getHdfsVolumePath());

      if (blockPackFuseConfig.getFsLocalIndex() != null) {
        ImmutableRoaringBitmapManager.setIndexDir(new File(blockPackFuseConfig.getFsLocalIndex()));
      }

      if (ENABLE_HDFS_LOGS) {
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          FileSystem fileSystem = path.getFileSystem(conf);
          Path logDirPath = new Path(path, "log");
          Path logPath = new Path(logDirPath, getHdfsLogName());
          Utils.setupHdfsLogger(fileSystem, logPath);
          cleanupOldLogFiles(fileSystem, logDirPath, Utils.getMaxHdfsLogFiles());
          LOGGER.info("hdfs logger setup");
          return null;
        });
      }

      HdfsSnapshotStrategy strategy = getHdfsSnapshotStrategy();

      LastestHdfsSnapshotStrategy.setMaxNumberOfMountSnapshots(blockPackFuseConfig.getNumberOfMountSnapshots());

      HdfsBlockStoreImplConfig config = HdfsBlockStoreImplConfig.DEFAULT_CONFIG;

      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        FileSystem fileSystem = path.getFileSystem(conf);

        LOGGER.info("add auto umount");
        addAutoUmount(blockPackFuseConfig.getFuseMountLocation());

        HdfsSnapshotUtil.enableSnapshots(fileSystem, path);
        HdfsSnapshotUtil.createSnapshot(fileSystem, path, HdfsSnapshotUtil.getMountSnapshotName(strategy));
        LOGGER.info("hdfs snapshot created");
        try (Closer closer = autoClose(Closer.create(), FUSE_CLOSE_PRIORITY)) {
          BlockPackAdmin blockPackAdmin = closer.register(BlockPackAdmin.getLoggerInstance(STATUS_LOGGER));
          LOGGER.info("admin server started");
          blockPackAdmin.setStatus(Status.INITIALIZATION);
          BlockPackFuseConfigInternalBuilder builder = BlockPackFuseConfigInternal.builder();
          SnapshotHandler snapshotHandler = name -> {
            UserGroupInformation ugiSnapshot = Utils.getUserGroupInformation();
            ugiSnapshot.doAs((PrivilegedExceptionAction<Void>) () -> {
              HdfsSnapshotUtil.createSnapshot(fileSystem, path, name);
              return null;
            });
          };
          BlockPackFuseConfigInternal fuseConfig = builder.blockPackAdmin(blockPackAdmin)
                                                          .path(path)
                                                          .config(config)
                                                          .blockPackFuseConfig(blockPackFuseConfig)
                                                          .blockStoreFactory(BlockStoreFactory.DEFAULT)
                                                          .fileSystem(fileSystem)
                                                          .snapshotHandler(snapshotHandler)
                                                          .build();

          BlockPackFuse blockPackFuse = closer.register(blockPackAdmin.register(new BlockPackFuse(fuseConfig)));
          LOGGER.info("block pack fuse created");
          blockPackFuse.mount();
        }
        return null;
      });
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void cleanupOldLogFiles(FileSystem fileSystem, Path logDirPath, int max)
      throws FileNotFoundException, IOException {
    String hostName = InetAddress.getLocalHost()
                                 .getHostName();
    FileStatus[] listStatus = fileSystem.listStatus(logDirPath, (PathFilter) path -> {
      String name = path.getName();
      return name.startsWith(hostName + "-") && name.endsWith(LOG);
    });

    List<Path> list = new ArrayList<>();
    for (FileStatus fileStatus : listStatus) {
      list.add(fileStatus.getPath());
    }

    Collections.sort(list, (o1, o2) -> o2.getName()
                                         .compareTo(o1.getName()));

    for (int i = max; i < list.size(); i++) {
      Path path = list.get(i);
      LOGGER.info("removing old hdfs log file {}", path);
      fileSystem.delete(path, false);
    }
  }

  private static String getHdfsLogName() throws IOException {
    String hostName = InetAddress.getLocalHost()
                                 .getHostName();
    SimpleDateFormat format = new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS);
    String ts = format.format(new Date());
    return hostName + "-" + ts + LOG;
  }

  private static void addAutoUmount(String fuseMountLocation) {
    File brickFile = new File(fuseMountLocation, "brick");
    ShutdownHookManager.get()
                       .addShutdownHook(() -> waitForUmount(brickFile.getAbsoluteFile()), AUTO_UMOUNT_PRIORITY);

  }

  private static void waitForUmount(File brickFile) {
    while (true) {
      try {
        Result result = ExecUtil.execAsResult(LOGGER, Level.DEBUG, SUDO, MOUNT);
        if (result.stdout.contains(brickFile.getCanonicalPath())) {
          if (ExecUtil.execReturnExitCode(LOGGER, Level.DEBUG, SUDO, UMOUNT, brickFile.getCanonicalPath()) != 0) {
            LOGGER.info("umount of {} failed", brickFile);
          }
        } else {
          return;
        }
      } catch (Throwable t) {
        LOGGER.error("Unknown error", t);
      }
      try {
        LOGGER.info("waiting for {} to umount", brickFile);
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  private static HdfsSnapshotStrategy getHdfsSnapshotStrategy() {
    return new LastestHdfsSnapshotStrategy();
  }

  private final BlockStore _blockStore;
  private final FuseFileSystemSingleMount _fuse;
  private final File _fuseLocalPath;
  private final Closer _closer;
  private final Thread _fuseMountThread;
  private final AtomicBoolean _closed = new AtomicBoolean(true);
  private final Path _path;
  private final MetricRegistry _registry = new MetricRegistry();
  private final CsvReporter _reporter;
  private final BlockPackAdmin _blockPackAdmin;
  private final PackLock _lock;
  private final Timer _timer;

  public BlockPackFuse(BlockPackFuseConfigInternal packFuseConfig) throws Exception {
    _timer = new Timer("SizeOfReport", true);
    _timer.schedule(getSizeOfReport(), TimeUnit.MINUTES.toMillis(1), TimeUnit.MINUTES.toMillis(1));
    BlockPackFuseConfig blockPackFuseConfig = packFuseConfig.getBlockPackFuseConfig();

    _blockPackAdmin = packFuseConfig.getBlockPackAdmin();
    _path = packFuseConfig.getPath();
    _blockPackAdmin.setStatus(Status.INITIALIZATION, "Creating Lock Manager");

    Path lockPath = Utils.getLockPathForVolumeMount(_path);
    LockLostAction lockLostAction = () -> {
      LOGGER.error("Mount lock lost for volume {}", _path);
      System.exit(0);
    };
    Configuration conf = packFuseConfig.getFileSystem()
                                       .getConf();
    _closer = Closer.create();
    _lock = _closer.register(PackLockFactory.create(conf, lockPath, lockLostAction));
    boolean lock = _lock.tryToLock();

    if (!lock) {
      for (int i = 0; i < 60 && !lock; i++) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        LOGGER.info("trying to lock volume {}", _path);
        lock = _lock.tryToLock();
      }
    }
    if (lock) {
      LOGGER.info("setting up metrics");
      File metricsDir = new File(blockPackFuseConfig.getFsMetricsLocation());
      metricsDir.mkdirs();
      _reporter = _closer.register(CsvReporter.forRegistry(_registry)
                                              .build(metricsDir));
      _reporter.start(1, TimeUnit.MINUTES);
      _fuseLocalPath = new File(blockPackFuseConfig.getFuseMountLocation());
      _fuseLocalPath.mkdirs();

      BlockStoreFactory factory = packFuseConfig.getBlockStoreFactory();
      LOGGER.info("creating block store");
      _blockStore = factory.getHdfsBlockStore(_blockPackAdmin, packFuseConfig, _registry);
      LOGGER.info("creating fuse file system");
      _fuse = _closer.register(new FuseFileSystemSingleMount(blockPackFuseConfig.getFuseMountLocation(), _blockStore,
          packFuseConfig.getSnapshotHandler()));
      _fuseMountThread = new Thread(() -> _fuse.localMount());
      _fuseMountThread.setName(FUSE_MOUNT_THREAD);
    } else {
      LOGGER.error("volume {} already is use.", _path);
      throw new IOException("volume " + _path + " already is use.");
    }
  }

  private TimerTask getSizeOfReport() {
    return new TimerTask() {
      @Override
      public void run() {
        if (_blockStore != null) {
          LOGGER.info("BlockStore {} contains {} memory", _blockStore, _blockStore.getSizeOf());
        }
      }
    };
  }

  FuseFileSystemSingleMount getFuse() {
    return _fuse;
  }

  @Override
  public void close() {
    LOGGER.info("close");
    synchronized (_closed) {
      if (!_closed.get()) {
        _fuseMountThread.interrupt();
        runQuietly(() -> {
          _closer.close();
          return null;
        });
        runQuietly(() -> {
          _fuseMountThread.join();
          return null;
        });
        _closed.set(true);
      }
    }
  }

  private void runQuietly(Callable<Void> runQuietly) {
    try {
      runQuietly.call();
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
    }
  }

  public void mount() throws IOException, InterruptedException {
    mount(true);
  }

  public void mount(boolean blocking) throws IOException, InterruptedException {
    _closed.set(false);
    startFuseMount();
    _blockPackAdmin.setStatus(Status.FUSE_MOUNT_STARTED, "Mounting FUSE @ " + _fuseLocalPath);
    LOGGER.info("fuse mount {} complete", _fuseLocalPath);
    waitUntilFuseIsMounted();
    _blockPackAdmin.setStatus(Status.FUSE_MOUNT_COMPLETE, "Mounting FUSE @ " + _fuseLocalPath);
    LOGGER.info("fuse mount {} visible", _fuseLocalPath);
    if (blocking) {
      _fuseMountThread.join();
    }
  }

  private void startFuseMount() {
    _fuseMountThread.start();
  }

  private void waitUntilFuseIsMounted() throws IOException {
    while (true) {
      LOGGER.info("waiting for fuse mount now exists {}", _fuseLocalPath);
      try {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      if (_fuseLocalPath.exists()) {
        LOGGER.info("fuse mount now exists {}", _fuseLocalPath);
        return;
      }
    }
  }

  private static Configuration getConfig() throws IOException {
    String hdfsConf = System.getProperty(HDFS_CONF);
    if (hdfsConf == null) {
      return new Configuration();
    } else {
      Configuration configuration = new Configuration(true);
      File file = new File(hdfsConf);
      if (file.exists() && file.isDirectory()) {
        for (File f : file.listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return name.endsWith(SITE_XML);
          }
        })) {
          configuration.addResource(new FileInputStream(f));
        }
      }
      return configuration;
    }
  }

}
