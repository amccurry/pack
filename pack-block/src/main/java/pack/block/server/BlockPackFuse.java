package pack.block.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.List;
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
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.io.Closer;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.util.HdfsSnapshotUtil;
import pack.block.fuse.FuseFileSystemSingleMount;
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

  private static final String BASH = "bash";
  private static final String PACK_LOG4J_CONFIG = "PACK_LOG4J_CONFIG";
  private static final String EXPORT = "export";
  private static final String SET_E = "set -e";
  private static final String SET_X = "set -x";
  private static final String POLLING_CLOSER = "polling-closer";
  private static final String PACK_LOG_DIR = "pack.log.dir";
  private static final String JAVA_PROPERTY = "-D";
  private static final String DOCKER_UNIX_SOCKET = "docker.unix.socket";
  private static final String LOG4J_FUSE_PROCESS_XML = "log4j-fuse-process.xml";
  private static final String LOCK = "/lock";
  private static final String BACKGROUND = "&";
  private static final String STDERR_REDIRECT = "2>";
  private static final String STDOUT_REDIRECT = ">";
  private static final String STDERR = "/stderr";
  private static final String STDOUT = "/stdout";
  private static final String INHERENT_ENV_VAR_SWITCH = "-E";
  private static final String SUDO = "sudo";
  private static final String ENV = "env";
  private static final String BIN_BASH = "#!/bin/bash";
  private static final String START_SH = "start.sh";
  private static final String NOHUP = "/bin/nohup";
  private static final String JAVA_HOME = "java.home";
  private static final String JAVA_CLASS_PATH = "java.class.path";
  private static final String BIN_JAVA = "/bin/java";
  private static final String XMX_SWITCH = "-Xmx128m";
  private static final String XMS_SWITCH = "-Xms128m";
  private static final String CLASSPATH_SWITCH = "-cp";
  private static final String MOUNT = "/mount";
  private static final String FUSE_MOUNT_THREAD = "fuse-mount-thread";
  private static final String SITE_XML = "-site.xml";
  private static final String HDFS_CONF = "hdfs-conf";

  public static Process startProcess(String fuseMountLocation, String fsMountLocation, String fsMetricsLocation,
      String fsLocalCache, String hdfVolumePath, String zkConnection, int zkTimeout, String volumeName,
      String logOutput, String unixSock, String libDir, int numberOfMountSnapshots, long volumeMissingPollingPeriod,
      int volumeMissingCountBeforeAutoShutdown, boolean countDockerDownAsMissing) throws IOException {
    String javaHome = System.getProperty(JAVA_HOME);

    String classPath = buildClassPath(System.getProperty(JAVA_CLASS_PATH), libDir);
    Builder<String> builder = ImmutableList.builder();

    String dockerUnixSocket = System.getProperty(DOCKER_UNIX_SOCKET);

    String zkTimeoutStr = Integer.toString(zkTimeout);
    builder.add(NOHUP)
           .add(javaHome + BIN_JAVA)
           .add(XMX_SWITCH)
           .add(XMS_SWITCH)
           .add(JAVA_PROPERTY + PACK_LOG_DIR + "=" + logOutput);
    if (dockerUnixSocket != null) {
      builder.add(JAVA_PROPERTY + DOCKER_UNIX_SOCKET + "=" + dockerUnixSocket);
    }
    builder.add(CLASSPATH_SWITCH)
           .add(classPath)
           .add(BlockPackFuse.class.getName())
           .add(volumeName)
           .add(fuseMountLocation)
           .add(fsMountLocation)
           .add(fsMetricsLocation)
           .add(fsLocalCache)
           .add(hdfVolumePath)
           .add(zkConnection)
           .add(zkTimeoutStr)
           .add(unixSock)
           .add(Integer.toString(numberOfMountSnapshots))
           .add(Long.toString(volumeMissingPollingPeriod))
           .add(Integer.toString(volumeMissingCountBeforeAutoShutdown))
           .add(Boolean.toString(countDockerDownAsMissing))
           .add(STDOUT_REDIRECT + logOutput + STDOUT)
           .add(STDERR_REDIRECT + logOutput + STDERR)
           .add(BACKGROUND)
           .build();
    ImmutableList<String> build = builder.build();
    String cmd = Joiner.on(' ')
                       .join(build);
    File logConfig = new File(logOutput, LOG4J_FUSE_PROCESS_XML);
    File start = new File(logOutput, START_SH);
    try (PrintWriter output = new PrintWriter(start)) {
      output.println(BIN_BASH);
      output.println(SET_X);
      output.println(SET_E);
      output.println(ENV);
      output.println(EXPORT + " " + PACK_LOG4J_CONFIG + "=" + logConfig.getAbsolutePath());
      IOUtils.write(cmd, output);
      output.println();
    }
    if (!logConfig.exists()) {
      try (InputStream inputStream = BlockPackFuse.class.getResourceAsStream("/" + LOG4J_FUSE_PROCESS_XML)) {
        try (FileOutputStream outputStream = new FileOutputStream(logConfig)) {
          IOUtils.copy(inputStream, outputStream);
        }
      }
    }

    LOGGER.info("Starting fuse mount from script file {}", start.getAbsolutePath());
    return new ProcessBuilder(SUDO, INHERENT_ENV_VAR_SWITCH, BASH, "-x", start.getAbsolutePath()).start();
  }

  private static String buildClassPath(String classPathProperty, String libDir) throws IOException {
    List<String> classPath = Splitter.on(':')
                                     .splitToList(classPathProperty);
    Builder<String> builder = ImmutableList.builder();
    for (String file : classPath) {
      File src = new File(file);
      File dest = new File(libDir, src.getName());
      if (src.exists()) {
        copy(src, dest);
        builder.add(dest.getAbsolutePath());
      }
    }
    return Joiner.on(':')
                 .join(builder.build());
  }

  private static void copy(File src, File dest) throws IOException {
    if (src.isDirectory()) {
      dest.mkdirs();
      for (File f : src.listFiles()) {
        copy(f, new File(dest, f.getName()));
      }
    } else {
      try (InputStream input = new BufferedInputStream(new FileInputStream(src))) {
        dest.delete();
        try (OutputStream output = new BufferedOutputStream(new FileOutputStream(dest))) {
          IOUtils.copy(input, output);
        }
      }
    }
  }

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
      UserGroupInformation ugi = Utils.getUserGroupInformation();
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        FileSystem fileSystem = FileSystem.get(conf);
        try (Closer closer = autoClose(Closer.create())) {
          BlockPackAdmin blockPackAdmin = closer.register(BlockPackAdminServer.startAdminServer(unixSock));
          blockPackAdmin.setStatus(Status.INITIALIZATION);
          BlockPackFuseConfig fuseConfig = BlockPackFuseConfig.builder()
                                                              .blockPackAdmin(blockPackAdmin)
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
                                                              .maxVolumeMissingCount(
                                                                  volumeMissingCountBeforeAutoShutdown)
                                                              .volumeMissingPollingPeriod(volumeMissingPollingPeriod)
                                                              .maxNumberOfMountSnapshots(numberOfMountSnapshots)
                                                              .countDockerDownAsMissing(countDockerDownAsMissing)
                                                              .build();
          HdfsSnapshotUtil.createSnapshot(fileSystem, path, HdfsSnapshotUtil.getMountSnapshotName());
          BlockPackFuse blockPackFuse = closer.register(blockPackAdmin.register(new BlockPackFuse(fuseConfig)));
          blockPackFuse.mount();
        }
        return null;
      });
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
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
    if (_lockManager.tryToLock(Utils.getLockName(_path))) {
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
  public void close() throws IOException {
    synchronized (_closed) {
      if (!_closed.get()) {
        if (_fileSystemMount) {
          if (_linuxFileSystem.isFstrimSupported()) {
            _blockPackAdmin.setStatus(Status.FS_TRIM_STARTED, "Fstrim started " + _fsLocalPath);
            _linuxFileSystem.fstrim(_fsLocalPath);
            _blockPackAdmin.setStatus(Status.FS_TRIM_COMPLETE, "Fstrim complete " + _fsLocalPath);
          }
          _blockPackAdmin.setStatus(Status.FS_UMOUNT_STARTED, "Unmounting " + _fsLocalPath);
          _linuxFileSystem.umount(_fsLocalPath);
          _blockPackAdmin.setStatus(Status.FS_UMOUNT_COMPLETE, "Unmounted " + _fsLocalPath);
        }
        _closer.close();
        _fuseMountThread.interrupt();
        try {
          _fuseMountThread.join();
        } catch (InterruptedException e) {
          LOGGER.info("Unknown error", e);
        }
        try {
          _lockManager.unlock(Utils.getLockName(_path));
        } catch (InterruptedException | KeeperException e) {
          LOGGER.debug("If zookeeper is closed already this node it cleaned up automatically.", e);
        }
        _closed.set(true);
      }
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
      try {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      if (_fuseLocalPath.exists()) {
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
