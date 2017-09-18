package pack.block.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.io.Closer;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.fuse.FuseFileSystemSingleMount;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.server.admin.Status;
import pack.block.server.fs.LinuxFileSystem;
import pack.block.util.Utils;
import pack.zk.utils.ZkUtils;
import pack.zk.utils.ZooKeeperClient;
import pack.zk.utils.ZooKeeperLockManager;

public class BlockPackFuse implements Closeable {

  private static final String LOG4J_FUSE_PROCESS_XML = "log4j-fuse-process.xml";

  private static <T extends Closeable> T autoClose(T t) {
    ShutdownHookManager.get()
                       .addShutdownHook(() -> {
                         LOGGER.info("Closing {} on shutdown", t);
                         IOUtils.closeQuietly(t);
                       }, Integer.MAX_VALUE);
    return t;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackFuse.class);

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
      String logOutput, String unixSock) throws IOException {
    String javaHome = System.getProperty(JAVA_HOME);
    String className = System.getProperty(JAVA_CLASS_PATH);
    Builder<String> builder = ImmutableList.builder();

    String zkTimeoutStr = Integer.toString(zkTimeout);
    builder.add(NOHUP)
           .add(javaHome + BIN_JAVA)
           .add(XMX_SWITCH)
           .add(XMS_SWITCH)
           .add("-Dpack.log.dir=" + logOutput)
           .add(CLASSPATH_SWITCH)
           .add(className)
           .add(BlockPackFuse.class.getName())
           .add(fuseMountLocation)
           .add(fsMountLocation)
           .add(fsMetricsLocation)
           .add(fsLocalCache)
           .add(hdfVolumePath)
           .add(zkConnection)
           .add(zkTimeoutStr)
           .add(unixSock)
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
      output.println(ENV);
      output.println("export PACK_LOG4J_CONFIG=" + logConfig.getAbsolutePath());
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
    return new ProcessBuilder(SUDO, INHERENT_ENV_VAR_SWITCH, "bash", "-x", start.getAbsolutePath()).start();
  }

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    Utils.setupLog4j();
    Configuration conf = getConfig();
    String fuseLocalPath = args[0];
    String fsLocalPath = args[1];
    String metricsLocalPath = args[2];
    String fsLocalCache = args[3];
    Path path = new Path(args[4]);
    String zkConnection = args[5];
    int zkTimeout = Integer.parseInt(args[6]);
    String unixSock = args[7];
    BlockPackAdmin blockPackAdmin = BlockPackAdmin.startAdminServer(unixSock);
    blockPackAdmin.setStatus(Status.INITIALIZATION);
    HdfsBlockStoreConfig config = HdfsBlockStoreConfig.DEFAULT_CONFIG;
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      FileSystem fileSystem = FileSystem.get(conf);
      try (Closer closer = autoClose(Closer.create())) {
        ZooKeeperClient zooKeeper = closer.register(ZkUtils.newZooKeeper(zkConnection, zkTimeout));

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
                                                            .zooKeeper(zooKeeper)
                                                            .fileSystemMount(true)
                                                            .blockStoreFactory(BlockStoreFactory.DEFAULT)
                                                            .build();
        BlockPackFuse blockPackFuse = closer.register(blockPackAdmin.register(new BlockPackFuse(fuseConfig)));
        blockPackFuse.mount();
      }
      return null;
    });
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

  public BlockPackFuse(BlockPackFuseConfig packFuseConfig) throws IOException {
    ZkUtils.mkNodesStr(packFuseConfig.getZooKeeper(), MOUNT + LOCK);
    _blockPackAdmin = packFuseConfig.getBlockPackAdmin();
    _ugi = packFuseConfig.getUgi();
    _fileSystemMount = packFuseConfig.isFileSystemMount();
    _path = packFuseConfig.getPath();
    _blockPackAdmin.setStatus(Status.INITIALIZATION, "Creating ZK Lock Manager");
    _lockManager = createLockmanager(packFuseConfig.getZooKeeper());
    _closer = Closer.create();
    _reporter = _closer.register(CsvReporter.forRegistry(_registry)
                                            .build(new File(packFuseConfig.getMetricsLocalPath())));
    _reporter.start(1, TimeUnit.MINUTES);
    _fuseLocalPath = new File(packFuseConfig.getFuseLocalPath());
    _fuseLocalPath.mkdirs();
    _fsLocalPath = new File(packFuseConfig.getFsLocalPath());
    _fsLocalPath.mkdirs();

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
    if (_lockManager.tryToLock(Utils.getLockName(_path))) {
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
        _blockPackAdmin.setStatus(Status.FS_MOUNT_STARTED, "Mounting " + device + " => " + _fsLocalPath);
        _linuxFileSystem.mount(device, _fsLocalPath);
        _blockPackAdmin.setStatus(Status.FS_MOUNT_COMPLETED, "Mounted " + device + " => " + _fsLocalPath);
        LOGGER.info("fs mount {} complete", _fsLocalPath);
      }
      if (blocking) {
        _fuseMountThread.join();
      }
    } else {
      LOGGER.error("volume {} already is use.", _path);
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

  public static ZooKeeperLockManager createLockmanager(ZooKeeperClient zooKeeper) {
    return new ZooKeeperLockManager(zooKeeper, MOUNT + LOCK);
  }
}
