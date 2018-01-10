package pack.block.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.PrivilegedAction;
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
import com.google.common.io.Closer;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.util.HdfsSnapshotUtil;
import pack.block.fuse.FuseFileSystemSingleMount;
import pack.block.server.BlockPackFuseConfig.BlockPackFuseConfigBuilder;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.server.admin.BlockPackAdminServer;
import pack.block.server.admin.DockerMonitor;
import pack.block.server.admin.Status;
import pack.block.server.admin.client.NoFileException;
import pack.block.server.fs.LinuxFileSystem;
import pack.block.util.Utils;
import pack.zk.utils.ZkUtils;
import pack.zk.utils.ZooKeeperClient;
import pack.zk.utils.ZooKeeperLockManager;

public class BlockPackFuse implements Closeable {

  private static <T extends Closeable> T autoClose(T t) {
    ShutdownHookManager.get()
                       .addShutdownHook(() -> {
                         LOGGER.info("Closing {} on shutdown", t);
                         IOUtils.closeQuietly(t);
                       }, Integer.MAX_VALUE);
    return t;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackFuse.class);

  private static final String DOCKER_UNIX_SOCKET = "docker.unix.socket";
  private static final String LOCK = "/lock";
  private static final String POLLING_CLOSER = "polling-closer";
  private static final String MOUNT = "/mount";
  private static final String FUSE_MOUNT_THREAD = "fuse-mount-thread";
  private static final String SITE_XML = "-site.xml";
  private static final String HDFS_CONF = "hdfs-conf";

  public static void main(String[] args) throws Exception {
    try {
      Utils.setupLog4j();
      Configuration conf = getConfig();
      String volumeName = args[0];
      String fuseLocalPath = args[1];
      String fsLocalPath = args[2];
      String metricsLocalPath = args[3];
      String fsLocalCache = args[4];
      Path path = new Path(args[5]);
      String zkConnection = args[6];
      int zkTimeout = Integer.parseInt(args[7]);
      String unixSock = args[8];
      int numberOfMountSnapshots = Integer.parseInt(args[9]);
      long volumeMissingPollingPeriod = Long.parseLong(args[10]);
      int volumeMissingCountBeforeAutoShutdown = Integer.parseInt(args[11]);
      boolean countDockerDownAsMissing = Boolean.parseBoolean(args[12]);

      HdfsBlockStoreConfig config = HdfsBlockStoreConfig.DEFAULT_CONFIG;

      // {
      // FileSystem fileSystem = FileSystem.get(conf);
      // HdfsSnapshotUtil.createSnapshot(fileSystem, path,
      // HdfsSnapshotUtil.getMountSnapshotName());
      // }

      UserGroupInformation ugi = Utils.getUserGroupInformation();
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        FileSystem fileSystem = FileSystem.get(conf);
        HdfsSnapshotUtil.createSnapshot(fileSystem, path, HdfsSnapshotUtil.getMountSnapshotName());
        try (Closer closer = autoClose(Closer.create())) {
          LOGGER.info("Using {} as unix domain socket", unixSock);
          BlockPackAdmin blockPackAdmin = closer.register(BlockPackAdminServer.startAdminServer(unixSock));
          blockPackAdmin.setStatus(Status.INITIALIZATION);
          BlockPackFuseConfigBuilder builder = BlockPackFuseConfig.builder();
          BlockPackFuseConfig fuseConfig = builder.blockPackAdmin(blockPackAdmin)
                                                  .ugi(ugi)
                                                  .fileSystem(fileSystem)
                                                  .path(path)
                                                  .config(config)
                                                  .fuseLocalPath(fuseLocalPath)
                                                  .fsLocalPath(fsLocalPath)
                                                  .metricsLocalPath(metricsLocalPath)
                                                  .fsLocalCache(fsLocalCache)
                                                  .zkConnectionString(zkConnection)
                                                  .zkSessionTimeout(zkTimeout)
                                                  .fileSystemMount(true)
                                                  .blockStoreFactory(BlockStoreFactory.DEFAULT)
                                                  .volumeName(volumeName)
                                                  .maxVolumeMissingCount(volumeMissingCountBeforeAutoShutdown)
                                                  .volumeMissingPollingPeriod(volumeMissingPollingPeriod)
                                                  .maxNumberOfMountSnapshots(numberOfMountSnapshots)
                                                  .countDockerDownAsMissing(countDockerDownAsMissing)
                                                  .build();

          BlockPackFuse blockPackFuse = closer.register(blockPackAdmin.register(new BlockPackFuse(fuseConfig)));
          blockPackFuse.mount();
        }
        return null;
      });
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      e.printStackTrace();
    }
  }

  private final HdfsBlockStore _blockStore;
  private final FuseFileSystemSingleMount _fuse;
  private final File _fuseLocalPath;
  private final File _fsLocalPath;
  private final LinuxFileSystem _linuxFileSystem;
  private final Closer _closer;
  private final HdfsMetaData _metaData;
  private final Thread _fuseMountThread;
  private final AtomicBoolean _closed = new AtomicBoolean(true);
  private final ZooKeeperLockManager _lockManager;
  private final Path _path;
  private final MetricRegistry _registry = new MetricRegistry();
  private final CsvReporter _reporter;
  private final boolean _fileSystemMount;
  private final UserGroupInformation _ugi;
  private final BlockPackAdmin _blockPackAdmin;
  private final int _maxVolumeMissingCount;
  private final Timer _timer;
  private final String _volumeName;
  private final FileSystem _fileSystem;
  private final int _maxNumberOfMountSnapshots;
  private final long _period;
  private final boolean _countDockerDownAsMissing;
  private final String _zkConnectionString;
  private final int _zkSessionTimeout;

  public BlockPackFuse(BlockPackFuseConfig packFuseConfig) throws Exception {
    _zkConnectionString = packFuseConfig.getZkConnectionString();
    _zkSessionTimeout = packFuseConfig.getZkSessionTimeout();
    try (ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper(_zkConnectionString, _zkSessionTimeout)) {
      ZkUtils.mkNodesStr(zooKeeper, MOUNT + LOCK);
    }
    _countDockerDownAsMissing = packFuseConfig.isCountDockerDownAsMissing();
    _period = packFuseConfig.getVolumeMissingPollingPeriod();
    _maxNumberOfMountSnapshots = packFuseConfig.getMaxNumberOfMountSnapshots();
    _blockPackAdmin = packFuseConfig.getBlockPackAdmin();
    _ugi = packFuseConfig.getUgi();
    _fileSystemMount = packFuseConfig.isFileSystemMount();
    _path = packFuseConfig.getPath();
    _blockPackAdmin.setStatus(Status.INITIALIZATION, "Creating ZK Lock Manager");
    _lockManager = createLockmanager(_zkConnectionString, _zkSessionTimeout);
    boolean lock = _lockManager.tryToLock(Utils.getLockName(_path));

    for (int i = 0; i < 10 && !lock; i++) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(6));
      LOGGER.info("trying to lock volume {}", _path);
      lock = _lockManager.tryToLock(Utils.getLockName(_path));
    }
    if (lock) {
      _closer = Closer.create();
      _reporter = _closer.register(CsvReporter.forRegistry(_registry)
                                              .build(new File(packFuseConfig.getMetricsLocalPath())));
      _reporter.start(1, TimeUnit.MINUTES);
      _fuseLocalPath = new File(packFuseConfig.getFuseLocalPath());
      _fuseLocalPath.mkdirs();
      _fsLocalPath = new File(packFuseConfig.getFsLocalPath());
      _fsLocalPath.mkdirs();

      _volumeName = packFuseConfig.getVolumeName();
      _maxVolumeMissingCount = packFuseConfig.getMaxVolumeMissingCount();
      _timer = new Timer(POLLING_CLOSER, true);
      _fileSystem = packFuseConfig.getFileSystem();

      BlockStoreFactory factory = packFuseConfig.getBlockStoreFactory();
      _blockStore = factory.getHdfsBlockStore(_blockPackAdmin, packFuseConfig, _ugi, _registry);
      _linuxFileSystem = _blockStore.getLinuxFileSystem();
      _metaData = _blockStore.getMetaData();
      _fuse = _closer.register(new FuseFileSystemSingleMount(packFuseConfig.getFuseLocalPath(), _blockStore));
      _fuseMountThread = new Thread(() -> _ugi.doAs((PrivilegedAction<Void>) () -> {
        _fuse.localMount();
        return null;
      }));
      _fuseMountThread.setName(FUSE_MOUNT_THREAD);
    } else {
      LOGGER.error("volume {} already is use.", _path);
      throw new IOException("volume " + _path + " already is use.");
    }
  }

  public static ZooKeeperLockManager createLockmanager(String zkConnectionString, int sessionTimeout)
      throws IOException {
    return ZkUtils.newZooKeeperLockManager(zkConnectionString, sessionTimeout, MOUNT + LOCK);
  }

  private void startDockerMonitorIfNeeded(long period) {
    if (System.getProperty(DOCKER_UNIX_SOCKET) != null) {
      DockerMonitor monitor = new DockerMonitor(new File(System.getProperty(DOCKER_UNIX_SOCKET)));
      _timer.schedule(getMonitorTimer(_volumeName, monitor), period, period);
    }
  }

  @Override
  public void close() {
    synchronized (_closed) {
      if (!_closed.get()) {
        if (_fileSystemMount) {
          if (_linuxFileSystem.isFstrimSupported()) {
            _blockPackAdmin.setStatus(Status.FS_TRIM_STARTED, "Fstrim started " + _fsLocalPath);
            runQuietly(() -> _linuxFileSystem.fstrim(_fsLocalPath));
            _blockPackAdmin.setStatus(Status.FS_TRIM_COMPLETE, "Fstrim complete " + _fsLocalPath);
          }
          _blockPackAdmin.setStatus(Status.FS_UMOUNT_STARTED, "Unmounting " + _fsLocalPath);
          runQuietly(() -> _linuxFileSystem.umount(_fsLocalPath));
          _blockPackAdmin.setStatus(Status.FS_UMOUNT_COMPLETE, "Unmounted " + _fsLocalPath);
        }
        runQuietly(() -> _closer.close());
        _fuseMountThread.interrupt();
        runQuietly(() -> _fuseMountThread.join());
        try {
          _lockManager.unlock(Utils.getLockName(_path));
        } catch (InterruptedException | KeeperException e) {
          LOGGER.debug("If zookeeper is closed already this node it cleaned up automatically.", e);
        }
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
    if (_fileSystemMount) {
      File device = new File(_fuseLocalPath, FuseFileSystemSingleMount.BRICK);
      if (!_linuxFileSystem.isFileSystemExists(device)) {
        LOGGER.info("file system does not exist on mount {} visible", _fuseLocalPath);
        int blockSize = _metaData.getFileSystemBlockSize();

        LOGGER.info("creating file system {} on mount {} visible", _linuxFileSystem, device);
        _blockPackAdmin.setStatus(Status.FS_MKFS, "Creating " + _linuxFileSystem.getType() + " @ " + device);
        _linuxFileSystem.mkfs(device, blockSize);
      }
      String mountOptions = _metaData.getMountOptions();
      _blockPackAdmin.setStatus(Status.FS_MOUNT_STARTED, "Mounting " + device + " => " + _fsLocalPath);
      _linuxFileSystem.mount(device, _fsLocalPath, mountOptions);
      _blockPackAdmin.setStatus(Status.FS_MOUNT_COMPLETED, "Mounted " + device + " => " + _fsLocalPath);
      LOGGER.info("fs mount {} complete", _fsLocalPath);
      HdfsSnapshotUtil.cleanupOldMountSnapshots(_fileSystem, _path, _maxNumberOfMountSnapshots);
    }
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
