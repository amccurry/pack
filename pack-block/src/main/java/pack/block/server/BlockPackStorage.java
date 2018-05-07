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

import pack.PackServer;
import pack.PackStorage;
import pack.block.blockstore.hdfs.CreateVolumeRequest;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import pack.block.blockstore.hdfs.util.HdfsSnapshotUtil;
import pack.block.blockstore.hdfs.util.LastestHdfsSnapshotStrategy;
import pack.block.fuse.FuseFileSystemSingleMount;
import pack.block.server.admin.Status;
import pack.block.server.admin.client.BlockPackAdminClient;
import pack.block.server.admin.client.ConnectionRefusedException;
import pack.block.server.admin.client.NoFileException;
import pack.block.server.fs.LinuxFileSystem;
import pack.json.Err;
import pack.json.MountUnmountRequest;
import pack.json.PathResponse;
import spark.Route;
import spark.Service;

public class BlockPackStorage implements PackStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackStorage.class);

  public static final String VOLUME_DRIVER_MOUNT_DEVICE = "/VolumeDriver.MountDevice";
  public static final String VOLUME_DRIVER_UNMOUNT_DEVICE = "/VolumeDriver.UnmountDevice";
  public static final String CLONE_PATH = "clonePath";
  public static final String SYMLINK_CLONE = "symlinkClone";
  public static final String MOUNT_COUNT = "mountCount";
  public static final String MOUNT = "/mount";
  public static final String METRICS = "metrics";
  public static final String SUDO = "sudo";
  public static final String SYNC = "sync";

  protected final Configuration _configuration;
  protected final Path _root;
  protected final UserGroupInformation _ugi;
  protected final File _localLogDir;
  protected final Set<String> _currentMountedVolumes = Collections.newSetFromMap(new ConcurrentHashMap<>());
  protected final String _zkConnection;
  protected final int _zkTimeout;
  protected final int _numberOfMountSnapshots;
  protected final long _volumeMissingPollingPeriod;
  protected final int _volumeMissingCountBeforeAutoShutdown;
  protected final boolean _countDockerDownAsMissing;
  protected final boolean _nohupProcess;
  protected final File _workingDir;
  protected final boolean _fileSystemMount;
  protected final HdfsSnapshotStrategy _snapshotStrategy;
  protected final Service _service;

  public BlockPackStorage(BlockPackStorageConfig config) throws IOException, InterruptedException {
    _service = config.getService();
    addServiceExtras(_service);
    _snapshotStrategy = config.getStrategy();
    _nohupProcess = config.isNohupProcess();
    _numberOfMountSnapshots = config.getNumberOfMountSnapshots();
    LastestHdfsSnapshotStrategy.setMaxNumberOfMountSnapshots(_numberOfMountSnapshots);
    _volumeMissingPollingPeriod = config.getVolumeMissingPollingPeriod();
    _volumeMissingCountBeforeAutoShutdown = config.getVolumeMissingCountBeforeAutoShutdown();
    _countDockerDownAsMissing = config.isCountDockerDownAsMissing();
    _fileSystemMount = config.isFileSystemMount();

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
    _zkConnection = config.getZkConnection();
    _zkTimeout = config.getZkTimeout();

    _configuration = config.getConfiguration();
    FileSystem fileSystem = getFileSystem(config.getRemotePath());
    Path remotePath = config.getRemotePath()
                            .makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    _root = remotePath;
    _ugi = config.getUgi();

    LOGGER.info("Creating hdfs root path {}", _root);
    _ugi.doAs(HdfsPriv.create(() -> getFileSystem(_root).mkdirs(_root)));

    _localLogDir = config.getLogDir();
    _localLogDir.mkdirs();

    _workingDir = config.getWorkingDir();
    _workingDir.mkdirs();

  }

  private void addServiceExtras(Service service) {
    service.post(VOLUME_DRIVER_MOUNT_DEVICE, (Route) (request, response) -> {
      PackServer.debugInfo(request);
      MountUnmountRequest mountUnmountRequest = PackServer.read(request, MountUnmountRequest.class);
      try {
        String deviceMountPoint = _ugi.doAs(
            (PrivilegedExceptionAction<String>) () -> mountVolume(mountUnmountRequest.getVolumeName(),
                mountUnmountRequest.getId(), true));
        return PathResponse.builder()
                           .mountpoint(deviceMountPoint)
                           .build();
      } catch (Throwable t) {
        LOGGER.error("error", t);
        return PathResponse.builder()
                           .mountpoint("<unknown>")
                           .error(t.getMessage())
                           .build();
      }
    }, PackServer.TRANSFORMER);

    service.post(VOLUME_DRIVER_UNMOUNT_DEVICE, (Route) (request, response) -> {
      PackServer.debugInfo(request);
      MountUnmountRequest mountUnmountRequest = PackServer.read(request, MountUnmountRequest.class);
      try {
        _ugi.doAs(HdfsPriv.create(
            () -> umountVolume(mountUnmountRequest.getVolumeName(), mountUnmountRequest.getId(), true)));
        return Err.builder()
                  .build();
      } catch (Throwable t) {
        LOGGER.error("error", t);
        return PathResponse.builder()
                           .mountpoint("<unknown>")
                           .error(t.getMessage())
                           .build();
      }
    }, PackServer.TRANSFORMER);
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
    return _ugi.doAs((PrivilegedExceptionAction<String>) () -> mountVolume(volumeName, id, false));
  }

  @Override
  public void unmount(String volumeName, String id) throws Exception {
    _ugi.doAs(HdfsPriv.create(() -> umountVolume(volumeName, id, false)));
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
    LOGGER.info("Create volume {}", volumeName);
    HdfsMetaData defaultmetaData = HdfsMetaData.DEFAULT_META_DATA;
    HdfsMetaData metaData = HdfsMetaData.setupOptions(defaultmetaData, options);

    Path volumePath = getVolumePath(volumeName);
    FileSystem fileSystem = getFileSystem(volumePath);

    CreateVolumeRequest request = CreateVolumeRequest.builder()
                                                     .metaData(metaData)
                                                     .volumeName(volumeName)
                                                     .volumePath(volumePath)
                                                     .clonePath(getClonePath(options))
                                                     .symlinkClone(getSymlinkClone(options))
                                                     .build();
    HdfsBlockStoreAdmin.createVolume(fileSystem, request);
  }

  private boolean getSymlinkClone(Map<String, Object> options) {
    Object object = options.get(SYMLINK_CLONE);
    if (object == null) {
      return false;
    }
    return Boolean.parseBoolean(object.toString()
                                      .toLowerCase());
  }

  private Path getClonePath(Map<String, Object> options) {
    Object object = options.get(CLONE_PATH);
    if (object == null) {
      return null;
    }
    return new Path(object.toString());
  }

  protected void removeVolume(String volumeName) throws IOException {
    LOGGER.info("Remove Volume {}", volumeName);
    Path volumePath = getVolumePath(volumeName);

    FileSystem fileSystem = getFileSystem(volumePath);
    try {
      HdfsSnapshotUtil.removeAllSnapshots(fileSystem, volumePath);
      HdfsSnapshotUtil.disableSnapshots(fileSystem, volumePath);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    fileSystem.delete(volumePath, true);
  }

  protected String mountVolume(String volumeName, String id, boolean deviceOnly)
      throws IOException, FileNotFoundException, InterruptedException, KeeperException {
    if (_fileSystemMount && deviceOnly) {
      throw new IOException("Device only mount, not supported with file system mount by external process.");
    }
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

    BlockPackFuseProcessBuilder.startProcess(_nohupProcess, localDevice.getAbsolutePath(),
        localFileSystemMount.getAbsolutePath(), localMetrics.getAbsolutePath(), localCache.getAbsolutePath(), path,
        _zkConnection, _zkTimeout, volumeName, logDir.getAbsolutePath(), unixSockFile.getAbsolutePath(),
        libDir.getAbsolutePath(), _numberOfMountSnapshots, _volumeMissingPollingPeriod,
        _volumeMissingCountBeforeAutoShutdown, _countDockerDownAsMissing, null, _fileSystemMount);

    if (_fileSystemMount) {
      waitForMount(localFileSystemMount, unixSockFile, Status.FS_MOUNT_COMPLETED);
      incrementMountCount(unixSockFile);
      return localFileSystemMount.getAbsolutePath();
    } else {
      FileSystem fileSystem = getFileSystem(volumePath);
      HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(fileSystem, volumePath);
      if (metaData == null) {
        throw new IOException("No metadata found for path " + volumePath);
      }
      waitForMount(localDevice, unixSockFile, Status.FUSE_MOUNT_COMPLETE);
      File device = new File(localDevice, FuseFileSystemSingleMount.BRICK);
      if (deviceOnly) {
        return device.getAbsolutePath();
      }
      mkfsIfNeeded(metaData, volumeName, device);
      mountFs(metaData, device, localFileSystemMount);
      HdfsSnapshotUtil.cleanupOldMountSnapshots(fileSystem, volumePath, _snapshotStrategy);
      incrementMountCount(unixSockFile);
      return localFileSystemMount.getAbsolutePath();
    }
  }

  private void umountFs(HdfsMetaData metaData, File localFileSystemMount) throws IOException {
    LinuxFileSystem linuxFileSystem = metaData.getFileSystemType()
                                              .getLinuxFileSystem();
    linuxFileSystem.umount(localFileSystemMount);
  }

  private void mountFs(HdfsMetaData metaData, File device, File localFileSystemMount) throws IOException {
    String mountOptions = metaData.getMountOptions();
    LinuxFileSystem linuxFileSystem = metaData.getFileSystemType()
                                              .getLinuxFileSystem();
    linuxFileSystem.mount(device, localFileSystemMount, mountOptions);
    if (linuxFileSystem.isFstrimSupported()) {
      linuxFileSystem.fstrim(localFileSystemMount);
    }
  }

  private void mkfsIfNeeded(HdfsMetaData metaData, String volumeName, File device) throws IOException {
    LinuxFileSystem linuxFileSystem = metaData.getFileSystemType()
                                              .getLinuxFileSystem();
    if (!linuxFileSystem.isFileSystemExists(device)) {
      linuxFileSystem.mkfs(device, metaData.getFileSystemBlockSize());
    }
  }

  private File getLibDir(String volumeName) {
    return new File(getVolumeDir(volumeName), "lib");
  }

  private File getVolumeDir(String volumeName) {
    return new File(_workingDir, "volumes/" + volumeName);
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
    return new File(getVolumeDir(volumeName), "sock");
  }

  private File getLocalCache(String volumeName) {
    return new File(getVolumeDir(volumeName), "cache");
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

  public static void waitForMount(File mountDir, File sockFile, Status desiredStatus)
      throws InterruptedException, IOException {
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    for (int i = 0; i < 30; i++) {
      if (sockFile.exists()) {
        break;
      }
      LOGGER.info("waiting for unix socket file to exist {}", sockFile);
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(1000));
    }
    BlockPackAdminClient client = BlockPackAdminClient.create(sockFile);
    while (true) {
      try {
        Status status = client.getStatus();
        if (status == desiredStatus) {
          LOGGER.info("startup complete {}", mountDir);
          return;
        }
        LOGGER.info("Waiting for status {}", mountDir, status);
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      } catch (NoFileException e) {
        throw new IOException("Unknown error while waiting on mount " + mountDir, e);
      }
    }
  }

  protected void umountVolume(String volumeName, String id, boolean deviceOnly)
      throws IOException, InterruptedException, FileNotFoundException, KeeperException {
    if (_fileSystemMount && deviceOnly) {
      throw new IOException("Device only mount, not supported with file system mount by external process.");
    }
    LOGGER.info("Unmount Volume {} Id {}", volumeName, id);
    File unixSockFile = getUnixSocketFile(volumeName);
    if (unixSockFile.exists()) {
      long count = decrementMountCount(unixSockFile);
      LOGGER.info("Mount count {}", count);
      if (count <= 0) {
        if (!_fileSystemMount && !deviceOnly) {
          Path volumePath = getVolumePath(volumeName);
          FileSystem fileSystem = getFileSystem(volumePath);
          HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(fileSystem, volumePath);
          if (metaData == null) {
            throw new IOException("No metadata found for path " + volumePath);
          }
          File localFileSystemMount = getLocalFileSystemMount(volumeName);
          umountFs(metaData, localFileSystemMount);
          sync();
        }
        try {
          shutdownVolume(unixSockFile);
        } catch (NoFileException e) {
          LOGGER.info("fuse process seems to be gone {}", unixSockFile);
          return;
        } catch (ConnectionRefusedException e) {
          LOGGER.info("fuse process seems to be gone {}", unixSockFile);
          return;
        } catch (IOException e) {
          if (e.getMessage()
               .trim()
               .toLowerCase()
               .equals("connection reset by peer")) {
            LOGGER.info("fuse process seems to be gone {}", unixSockFile);
            return;
          }
          throw e;
        }
      }
    }
  }

  private void sync() throws IOException {
    // Utils.exec(LOGGER, SUDO, SYNC);
  }

  private long decrementMountCount(File unixSockFile) throws IOException {
    BlockPackAdminClient client = BlockPackAdminClient.create(unixSockFile);
    return client.decrementCounter(MOUNT_COUNT);
  }

  private long incrementMountCount(File unixSockFile) throws IOException {
    BlockPackAdminClient client = BlockPackAdminClient.create(unixSockFile);
    return client.incrementCounter(MOUNT_COUNT);
  }

  public static void shutdownVolume(File unixSockFile) throws IOException, InterruptedException {
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
    return new File(getVolumeDir(volumeName), "fs");
  }

  private File getLocalDevice(String volumeName) {
    return new File(getVolumeDir(volumeName), "dev");
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
