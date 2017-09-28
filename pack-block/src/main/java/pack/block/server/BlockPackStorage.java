package pack.block.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;

import pack.PackStorage;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.server.admin.Status;
import pack.block.server.admin.client.BlockPackAdminClient;
import pack.block.server.admin.client.ConnectionRefusedException;
import pack.block.server.admin.client.NoFileException;
import pack.block.server.fs.FileSystemType;
import pack.zk.utils.ZooKeeperClient;
import pack.zk.utils.ZooKeeperLockManager;

public class BlockPackStorage implements PackStorage {

  private static final String MOUNT_COUNT = "mountCount";

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackStorage.class);

  public static final String MOUNT = "/mount";

  private static final String METRICS = "metrics";

  protected final Configuration _configuration;
  protected final Path _root;
  protected final UserGroupInformation _ugi;
  protected final File _localMountCountDir;
  protected final File _localFileSystemDir;
  protected final File _localDeviceDir;
  protected final File _localLogDir;
  protected final File _localCacheDir;
  protected final File _localUnixSocketDir;
  protected final File _localLibDir;
  protected final Set<String> _currentMountedVolumes = Collections.newSetFromMap(new ConcurrentHashMap<>());
  protected final String _zkConnection;
  protected final int _zkTimeout;

  public BlockPackStorage(File workingDir, File logDir, Configuration configuration, Path remotePath,
      UserGroupInformation ugi, String zkConnection, int zkTimeout) throws IOException, InterruptedException {
    Closer closer = Closer.create();
    closer.register((Closeable) () -> {
      for (String volumeName : _currentMountedVolumes) {
        try {
          unmount(volumeName, null);
        } catch (Exception e) {
          LOGGER.error("Unknown error while trying to umount volume " + volumeName);
        }
      }
    });
    addShutdownHook(closer);
    _zkConnection = zkConnection;
    _zkTimeout = zkTimeout;

    _configuration = configuration;
    FileSystem fileSystem = getFileSystem(remotePath);
    remotePath = remotePath.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    _root = remotePath;
    _ugi = ugi;

    LOGGER.info("Creating hdfs root path {}", _root);
    _ugi.doAs(HdfsPriv.create(() -> getFileSystem(_root).mkdirs(_root)));

    _localLogDir = logDir;
    _localLogDir.mkdirs();

    _localLibDir = new File(workingDir, "lib");
    _localLibDir.mkdirs();
    _localFileSystemDir = new File(workingDir, "fs");
    _localFileSystemDir.mkdirs();
    _localMountCountDir = new File(workingDir, "counts");
    _localMountCountDir.mkdirs();
    _localDeviceDir = new File(workingDir, "devices");
    _localDeviceDir.mkdirs();
    _localCacheDir = new File(workingDir, "cache");
    _localCacheDir.mkdirs();
    _localUnixSocketDir = new File(workingDir, "sock");
    _localUnixSocketDir.mkdirs();
  }

  public static ZooKeeperLockManager createLockmanager(ZooKeeperClient zooKeeper) {
    return new ZooKeeperLockManager(zooKeeper, MOUNT + "/lock");
  }

  private void addShutdownHook(Closer closer) {
    Runtime.getRuntime()
           .addShutdownHook(new Thread(() -> {
             try {
               closer.close();
             } catch (IOException e) {
               LOGGER.error("Unknown error while trying to umount volumes");
             }
           }));
  }

  @Override
  public void create(String volumeName, Map<String, Object> options) throws Exception {
    _ugi.doAs(HdfsPriv.create(() -> createVolume(volumeName, options)));
  }

  @Override
  public void remove(String volumeName) throws Exception {
    _ugi.doAs(HdfsPriv.create(() -> removeVolume(volumeName)));
  }

  @Override
  public String mount(String volumeName, String id) throws Exception {
    return _ugi.doAs((PrivilegedExceptionAction<String>) () -> mountVolume(volumeName, id));
  }

  @Override
  public void unmount(String volumeName, String id) throws Exception {
    _ugi.doAs(HdfsPriv.create(() -> umountVolume(volumeName, id)));
  }

  @Override
  public boolean exists(String volumeName) throws Exception {
    return _ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> existsVolume(volumeName));
  }

  @Override
  public String getMountPoint(String volumeName) throws IOException {
    File localFileSystemMount = getLocalFileSystemMount(volumeName);
    LOGGER.info("Get MountPoint volume {} path {}", volumeName, localFileSystemMount);
    File unixSockFile = getUnixSocketFile(volumeName);
    LOGGER.info("Volume {} localCache {}", volumeName, unixSockFile);
    if (isMounted(unixSockFile)) {
      return localFileSystemMount.getAbsolutePath();
    }
    return null;
  }

  @Override
  public List<String> listVolumes() throws Exception {
    return _ugi.doAs((PrivilegedExceptionAction<List<String>>) () -> listHdfsVolumes());
  }

  protected List<String> listHdfsVolumes() throws IOException, FileNotFoundException {
    LOGGER.info("List Volumes");
    FileSystem fileSystem = getFileSystem(_root);
    FileStatus[] listStatus = fileSystem.listStatus(_root);
    List<String> result = new ArrayList<>();
    for (FileStatus fileStatus : listStatus) {
      result.add(fileStatus.getPath()
                           .getName());
    }
    return result;
  }

  protected boolean existsVolume(String volumeName) throws IOException {
    LOGGER.info("exists {}", volumeName);
    FileSystem fileSystem = getFileSystem(_root);
    return fileSystem.exists(getVolumePath(volumeName));
  }

  protected void createVolume(String volumeName, Map<String, Object> options) throws IOException {
    Path volumePath = getVolumePath(volumeName);
    FileSystem fileSystem = getFileSystem(volumePath);
    if (!fileSystem.exists(volumePath)) {
      if (fileSystem.mkdirs(volumePath)) {
        LOGGER.info("Create volume {}", volumeName);
        HdfsMetaData defaultmetaData = HdfsMetaData.DEFAULT_META_DATA;
        HdfsMetaData metaData = HdfsMetaData.setupOptions(defaultmetaData, options);
        LOGGER.info("HdfsMetaData volume {} {}", volumeName, metaData);
        HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, volumePath);
      } else {
        LOGGER.info("Create not created volume {}", volumeName);
      }
    }
  }

  protected void removeVolume(String volumeName) throws IOException {
    LOGGER.info("Remove Volume {}", volumeName);
    Path volumePath = getVolumePath(volumeName);

    FileSystem fileSystem = getFileSystem(volumePath);
    fileSystem.delete(volumePath, true);
  }

  protected String mountVolume(String volumeName, String id)
      throws IOException, FileNotFoundException, InterruptedException, KeeperException {
    createVolume(volumeName, ImmutableMap.of());
    LOGGER.info("Mount Id {} volumeName {}", id, volumeName);
    File logDir = getLogDir(volumeName);
    LOGGER.info("Mount Id {} logDir {}", id, logDir);
    Path volumePath = getVolumePath(volumeName);
    LOGGER.info("Mount Id {} volumePath {}", id, volumePath);
    File localFileSystemMount = getLocalFileSystemMount(volumeName);
    LOGGER.info("Mount Id {} localFileSystemMount {}", id, localFileSystemMount);
    File localDevice = getLocalDevice(volumeName);
    LOGGER.info("Mount Id {} localDevice {}", id, localDevice);
    File localMetrics = getLocalMetrics(logDir);
    LOGGER.info("Mount Id {} localMetrics {}", id, localMetrics);
    File localCache = getLocalCache(volumeName);
    LOGGER.info("Mount Id {} localCache {}", id, localCache);
    File unixSockFile = getUnixSocketFile(volumeName);
    LOGGER.info("Mount Id {} unixSockFile {}", id, unixSockFile);
    File libDir = getLibDir(volumeName);
    LOGGER.info("Mount Id {} libDir {}", id, libDir);

    libDir.mkdirs();
    localCache.mkdirs();
    localFileSystemMount.mkdirs();
    localDevice.mkdirs();
    localMetrics.mkdirs();

    if (isMounted(unixSockFile)) {
      incrementMountCount(unixSockFile);
      return localFileSystemMount.getAbsolutePath();
    }

    if (unixSockFile.exists()) {
      unixSockFile.delete();
    }

    String path = volumePath.toUri()
                            .getPath();

    BlockPackFuse.startProcess(localDevice.getAbsolutePath(), localFileSystemMount.getAbsolutePath(),
        localMetrics.getAbsolutePath(), localCache.getAbsolutePath(), path, _zkConnection, _zkTimeout, volumeName,
        logDir.getAbsolutePath(), unixSockFile.getAbsolutePath(), libDir.getAbsolutePath());

    waitForMount(localFileSystemMount, unixSockFile);
    incrementMountCount(unixSockFile);
    return localFileSystemMount.getAbsolutePath();
  }

  private File getLibDir(String volumeName) {
    return new File(_localLibDir, volumeName);
  }

  private boolean isMounted(File unixSockFile) throws IOException {
    BlockPackAdminClient client = BlockPackAdminClient.create(unixSockFile);
    try {
      client.getPid();
      return true;
    } catch (NoFileException e) {
      return false;
    } catch (ConnectionRefusedException e) {
      return false;
    }
  }

  private File getUnixSocketFile(String volumeName) {
    return new File(_localUnixSocketDir, volumeName);
  }

  private File getLocalCache(String volumeName) {
    return new File(_localCacheDir, volumeName);
  }

  private File getLocalMetrics(File logDir) {
    File file = new File(logDir, METRICS);
    file.mkdirs();
    return file;
  }

  private File getLogDir(String volumeName) {
    File logDir = new File(_localLogDir, volumeName);
    logDir.mkdirs();
    return logDir;
  }

  private void waitForMount(File localFileSystemMount, File sockFile) throws InterruptedException, IOException {
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    while (true) {
      if (sockFile.exists()) {
        break;
      }
      LOGGER.info("waiting for unix socket file to exist {}", sockFile);
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    }
    BlockPackAdminClient client = BlockPackAdminClient.create(sockFile);
    while (true) {
      Status status = client.getStatus();
      if (status == Status.FS_MOUNT_COMPLETED) {
        LOGGER.info("mount complete {}", localFileSystemMount);
        return;
      }
      LOGGER.info("Waiting for mount {} status {}", localFileSystemMount, status);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
  }

  protected void umountVolume(String volumeName, String id)
      throws IOException, InterruptedException, FileNotFoundException, KeeperException {
    LOGGER.info("Unmount Volume {} Id {}", volumeName, id);
    File unixSockFile = getUnixSocketFile(volumeName);
    long count = decrementMountCount(unixSockFile);
    LOGGER.info("Mount count {}", count);
    if (count <= 0) {
      try {
        umountVolume(unixSockFile);
      } catch (NoFileException e) {
        LOGGER.info("fuse process seems to be gone {}", unixSockFile);
        return;
      } catch (ConnectionRefusedException e) {
        LOGGER.info("fuse process seems to be gone {}", unixSockFile);
        return;
      }
    }
  }

  private long decrementMountCount(File unixSockFile) throws IOException {
    BlockPackAdminClient client = BlockPackAdminClient.create(unixSockFile);
    return client.decrementCounter(MOUNT_COUNT);
  }

  private long incrementMountCount(File unixSockFile) throws IOException {
    BlockPackAdminClient client = BlockPackAdminClient.create(unixSockFile);
    return client.incrementCounter(MOUNT_COUNT);
  }

  private void umountVolume(File unixSockFile) throws IOException, InterruptedException {
    BlockPackAdminClient client = BlockPackAdminClient.create(unixSockFile);
    while (true) {
      Status status = client.getStatus();
      switch (status) {
      case FS_MKFS:
      case FS_MOUNT_COMPLETED:
      case FS_MOUNT_STARTED:
        client.umount();
        break;
      case FS_UMOUNT_STARTED:
        break;
      case FS_UMOUNT_COMPLETE:
      case UNKNOWN:
      case FUSE_MOUNT_COMPLETE:
      case FUSE_MOUNT_STARTED:
      case FS_TRIM_STARTED:
      case FS_TRIM_COMPLETE:
      case INITIALIZATION:
        client.shutdown();
        break;
      default:
        break;
      }
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
  }

  private File getLocalFileSystemMount(String volumeName) {
    return new File(_localFileSystemDir, volumeName);
  }

  private File getLocalDevice(String volumeName) {
    return new File(_localDeviceDir, volumeName);
  }

  private FileSystem getFileSystem(Path path) throws IOException {
    return path.getFileSystem(_configuration);
  }

  static class HdfsPriv implements PrivilegedExceptionAction<Void> {

    private final Exec exec;

    private HdfsPriv(Exec exec) {
      this.exec = exec;
    }

    @Override
    public Void run() throws Exception {
      exec.exec();
      return null;
    }

    static PrivilegedExceptionAction<Void> create(Exec exec) {
      return new HdfsPriv(exec);
    }

  }

  static interface Exec {
    void exec() throws Exception;
  }

  private Path getVolumePath(String volumeName) {
    return new Path(_root, volumeName);
  }
}
