package pack.block.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Closer;

import pack.PackServer.Result;
import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import pack.block.blockstore.hdfs.util.HdfsSnapshotUtil;
import pack.block.blockstore.hdfs.util.LastestHdfsSnapshotStrategy;
import pack.block.fuse.FuseFileSystemSingleMount;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.server.admin.BlockPackAdminServer;
import pack.block.server.admin.DockerMonitor;
import pack.block.server.admin.Status;
import pack.block.server.admin.client.NoFileException;
import pack.block.server.json.BlockPackFuseConfig;
import pack.block.server.json.BlockPackFuseConfigInternal;
import pack.block.server.json.BlockPackFuseConfigInternal.BlockPackFuseConfigInternalBuilder;
import pack.block.util.Utils;
import pack.zk.utils.ZkUtils;
import pack.zk.utils.ZooKeeperClientFactory;
import pack.zk.utils.ZooKeeperLockManager;

public class BlockPackFuse implements Closeable {

  private static <T extends Closeable> T autoClose(T t, int priority) {
    ShutdownHookManager.get()
                       .addShutdownHook(() -> {
                         IOUtils.closeQuietly(t);
                       }, priority);
    return t;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackFuse.class);

  private static final String DOCKER_UNIX_SOCKET = "docker.unix.socket";
  private static final String MOUNT_ZK = "/mount";
  private static final String POLLING_CLOSER = "polling-closer";
  private static final String MOUNT = "mount";
  private static final String SUDO = "sudo";
  private static final String UMOUNT = "umount";

  private static final String FUSE_MOUNT_THREAD = "fuse-mount-thread";
  private static final String SITE_XML = "-site.xml";
  private static final String HDFS_CONF = "hdfs-conf";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int AUTO_UMOUNT_PRIORITY = 10000;
  private static final int FUSE_CLOSE_PRIORITY = 9000;
  // private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

  public static void main(String[] args) throws Exception {
    try {
      Utils.setupLog4j();
      LOGGER.info("log4j setup");
      Configuration conf = getConfig();
      LOGGER.info("hdfs config read");

      BlockPackFuseConfig blockPackFuseConfig = MAPPER.readValue(new File(args[0]), BlockPackFuseConfig.class);
      Path path = new Path(blockPackFuseConfig.getHdfsVolumePath());
      String unixSock = blockPackFuseConfig.getUnixSock();

      HdfsSnapshotStrategy strategy = getHdfsSnapshotStrategy();

      LastestHdfsSnapshotStrategy.setMaxNumberOfMountSnapshots(blockPackFuseConfig.getNumberOfMountSnapshots());

      HdfsBlockStoreConfig config = HdfsBlockStoreConfig.DEFAULT_CONFIG;

      UserGroupInformation ugi = Utils.getUserGroupInformation();
      LOGGER.info("hdfs ugi created");
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        FileSystem fileSystem = path.getFileSystem(conf);

        LOGGER.info("add auto umount");
        addAutoUmount(blockPackFuseConfig.getFuseMountLocation());

        HdfsSnapshotUtil.enableSnapshots(fileSystem, path);
        HdfsSnapshotUtil.createSnapshot(fileSystem, path, HdfsSnapshotUtil.getMountSnapshotName(strategy));
        LOGGER.info("hdfs snapshot created");
        try (Closer closer = autoClose(Closer.create(), FUSE_CLOSE_PRIORITY)) {
          LOGGER.info("Using {} as unix domain socket", unixSock);
          BlockPackAdmin blockPackAdmin = closer.register(BlockPackAdminServer.startAdminServer(unixSock));
          LOGGER.info("admin server started");
          blockPackAdmin.setStatus(Status.INITIALIZATION);
          BlockPackFuseConfigInternalBuilder builder = BlockPackFuseConfigInternal.builder();
          BlockPackFuseConfigInternal fuseConfig = builder.blockPackAdmin(blockPackAdmin)
                                                          .path(path)
                                                          .config(config)
                                                          .blockPackFuseConfig(blockPackFuseConfig)
                                                          .blockStoreFactory(BlockStoreFactory.DEFAULT)
                                                          .fileSystem(fileSystem)
                                                          .build();

          BlockPackFuse blockPackFuse = closer.register(blockPackAdmin.register(new BlockPackFuse(fuseConfig)));
          // closer.register((Closeable) () -> {
          // RUNNING.set(false);
          // });
          LOGGER.info("block pack fuse created");
          blockPackFuse.mount();
          // blockPackFuse.mount(false);
          // while (RUNNING.get()) {
          // Thread.sleep(TimeUnit.SECONDS.toMillis(10));
          // }
        }
        return null;
      });
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void addAutoUmount(String fuseMountLocation) {
    File brickFile = new File(fuseMountLocation, "brick");
    ShutdownHookManager.get()
                       .addShutdownHook(() -> waitForUmount(brickFile.getAbsoluteFile()), AUTO_UMOUNT_PRIORITY);

  }

  private static void waitForUmount(File brickFile) {
    while (true) {
      try {
        Result result = Utils.execAsResultQuietly(LOGGER, SUDO, MOUNT);
        if (result.stdout.contains(brickFile.getCanonicalPath())) {
          if (Utils.execReturnExitCode(LOGGER, SUDO, UMOUNT, brickFile.getCanonicalPath()) != 0) {
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

  private final HdfsBlockStore _blockStore;
  private final FuseFileSystemSingleMount _fuse;
  private final File _fuseLocalPath;
  private final Closer _closer;
  private final Thread _fuseMountThread;
  private final AtomicBoolean _closed = new AtomicBoolean(true);
  private final ZooKeeperLockManager _lockManager;
  private final Path _path;
  private final MetricRegistry _registry = new MetricRegistry();
  private final CsvReporter _reporter;
  private final BlockPackAdmin _blockPackAdmin;
  private final int _maxVolumeMissingCount;
  private final Timer _timer;
  private final String _volumeName;
  private final long _period;
  private final boolean _countDockerDownAsMissing;
  private final ZooKeeperClientFactory _zk;

  public BlockPackFuse(BlockPackFuseConfigInternal packFuseConfig) throws Exception {
    BlockPackFuseConfig blockPackFuseConfig = packFuseConfig.getBlockPackFuseConfig();
    String zkConnectionString = blockPackFuseConfig.getZkConnection();
    int zkSessionTimeout = blockPackFuseConfig.getZkTimeout();
    _zk = ZkUtils.newZooKeeperClientFactory(zkConnectionString, zkSessionTimeout);
    _countDockerDownAsMissing = blockPackFuseConfig.isCountDockerDownAsMissing();
    _period = blockPackFuseConfig.getVolumeMissingPollingPeriod();
    _blockPackAdmin = packFuseConfig.getBlockPackAdmin();
    _path = packFuseConfig.getPath();
    _blockPackAdmin.setStatus(Status.INITIALIZATION, "Creating ZK Lock Manager");
    LOGGER.info("trying to obtain lock");
    _lockManager = createLockmanager(_zk, packFuseConfig.getPath()
                                                        .getName());
    boolean lock = _lockManager.tryToLock(Utils.getLockName(_path));

    if (!lock) {
      for (int i = 0; i < 60 && !lock; i++) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        LOGGER.info("trying to lock volume {}", _path);
        lock = _lockManager.tryToLock(Utils.getLockName(_path));
      }
    }
    if (lock) {
      _closer = Closer.create();
      LOGGER.info("setting up metrics");
      File metricsDir = new File(blockPackFuseConfig.getFsMetricsLocation());
      metricsDir.mkdirs();
      _reporter = _closer.register(CsvReporter.forRegistry(_registry)
                                              .build(metricsDir));
      _reporter.start(1, TimeUnit.MINUTES);
      _fuseLocalPath = new File(blockPackFuseConfig.getFuseMountLocation());
      _fuseLocalPath.mkdirs();

      _volumeName = blockPackFuseConfig.getVolumeName();
      _maxVolumeMissingCount = blockPackFuseConfig.getVolumeMissingCountBeforeAutoShutdown();
      _timer = new Timer(POLLING_CLOSER, true);

      BlockStoreFactory factory = packFuseConfig.getBlockStoreFactory();
      LOGGER.info("creating block store");
      _blockStore = factory.getHdfsBlockStore(_blockPackAdmin, packFuseConfig, _registry);
      LOGGER.info("creating fuse file system");
      _fuse = _closer.register(new FuseFileSystemSingleMount(blockPackFuseConfig.getFuseMountLocation(), _blockStore));
      _fuseMountThread = new Thread(() -> _fuse.localMount());
      _fuseMountThread.setName(FUSE_MOUNT_THREAD);
    } else {
      LOGGER.error("volume {} already is use.", _path);
      throw new IOException("volume " + _path + " already is use.");
    }
  }

  public static ZooKeeperLockManager createLockmanager(ZooKeeperClientFactory zk, String name) throws IOException {
    return ZkUtils.newZooKeeperLockManager(zk, MOUNT_ZK + "/" + name);
  }

  private void startDockerMonitorIfNeeded(long period) {
    if (System.getProperty(DOCKER_UNIX_SOCKET) != null) {
      DockerMonitor monitor = new DockerMonitor(new File(System.getProperty(DOCKER_UNIX_SOCKET)));
      _timer.schedule(getMonitorTimer(_volumeName, monitor), period, period);
    }
  }

  @Override
  public void close() {
    LOGGER.info("close");
    synchronized (_closed) {
      if (!_closed.get()) {
        runQuietly(() -> _closer.close());
        _fuseMountThread.interrupt();
        runQuietly(() -> _fuseMountThread.join());
        try {
          _lockManager.unlock(Utils.getLockName(_path));
        } catch (IOException | InterruptedException | KeeperException e) {
          LOGGER.debug("If zookeeper is closed already this node it cleaned up automatically.", e);
        }
        runQuietly(() -> _zk.close());
        _closed.set(true);
      }
    }
  }

  private void runQuietly(RunQuietly runQuietly) {
    try {
      runQuietly.run();
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
    }
  }

  public void mount() throws IOException, InterruptedException, KeeperException {
    mount(true);
  }

  public void mount(boolean blocking) throws IOException, InterruptedException, KeeperException {
    _closed.set(false);
    startFuseMount();
    _blockPackAdmin.setStatus(Status.FUSE_MOUNT_STARTED, "Mounting FUSE @ " + _fuseLocalPath);
    LOGGER.info("fuse mount {} complete", _fuseLocalPath);
    waitUntilFuseIsMounted();
    _blockPackAdmin.setStatus(Status.FUSE_MOUNT_COMPLETE, "Mounting FUSE @ " + _fuseLocalPath);
    LOGGER.info("fuse mount {} visible", _fuseLocalPath);
    startDockerMonitorIfNeeded(_period);
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

  private TimerTask getMonitorTimer(String volumeName, DockerMonitor monitor) {
    return new TimerTask() {
      private int count = 0;

      @Override
      public void run() {
        try {
          if (!isVolumeStillInUse(monitor, volumeName)) {
            count++;
            checkMissingCount();
          } else {
            count = 0;
          }
        } catch (Exception e) {
          if (_countDockerDownAsMissing && e instanceof NoFileException) {
            count++;
            checkMissingCount();
          } else {
            LOGGER.error("Unknown error", e);
          }
        }
      }

      private void checkMissingCount() {
        if (count >= _maxVolumeMissingCount) {
          LOGGER.info("Volume {} no longer in use, closing.", _volumeName);
          Utils.shutdownProcess(BlockPackFuse.this);
        } else {
          LOGGER.info("Volume {} no longer in use, current recheck count {} of max {}.", _volumeName, count,
              _maxVolumeMissingCount);
        }
      }

      private boolean isVolumeStillInUse(DockerMonitor monitor, String volumeName) throws IOException {
        int containerCount = monitor.getContainerCount(volumeName);
        LOGGER.debug("Volume still has {} containers using mount", containerCount);
        return containerCount > 0;
      }
    };
  }
}
