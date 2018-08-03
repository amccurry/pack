package pack.block.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;

import pack.PackServer;
import pack.PackServer.Result;
import pack.PackStorage;
import pack.block.blockstore.hdfs.CreateVolumeRequest;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.lock.HdfsLock;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import pack.block.blockstore.hdfs.util.HdfsSnapshotUtil;
import pack.block.blockstore.hdfs.util.LastestHdfsSnapshotStrategy;
import pack.block.fuse.FuseFileSystemSingleMount;
import pack.block.server.fs.LinuxFileSystem;
import pack.block.server.json.BlockPackFuseConfig;
import pack.block.server.json.BlockPackFuseConfig.BlockPackFuseConfigBuilder;
import pack.block.util.Utils;
import pack.json.Err;
import pack.json.MountUnmountRequest;
import pack.json.PathResponse;
import spark.Route;
import spark.Service;

public class BlockPackStorage implements PackStorage {

  private static final String LOCKED = "locked";

  private static final String UNLOCKED = "unlocked";

  private static final String BRICK = "brick";

  private static final String VOLUMES = "volumes";

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackStorage.class);

  public static final String VOLUME_DRIVER_MOUNT_DEVICE = "/VolumeDriver.MountDevice";
  public static final String VOLUME_DRIVER_UNMOUNT_DEVICE = "/VolumeDriver.UnmountDevice";
  public static final String CLONE_PATH = "clonePath";
  public static final String SYMLINK_CLONE = "symlinkClone";
  public static final String MOUNT_COUNT = "mountCount";
  public static final String MOUNT = "mount";
  public static final String METRICS = "metrics";
  public static final String SUDO = "sudo";
  public static final String SYNC = "sync";
  public static final String CONFIG_JSON = "config.json";
  private static final String FS = "fs";
  private static final String DEV = "dev";
  private static final String SHUTDOWN = "shutdown";
  private static final String Q = "-q";
  private static final String NO_TRUNC = "--no-trunc";
  private static final String PS = "ps";
  private static final String DOCKER = "docker";
  private static final long MAX_AGE = TimeUnit.MINUTES.toMillis(5);

  protected final Configuration _configuration;
  protected final Path _root;
  protected final File _localLogDir;
  protected final Set<String> _currentMountedVolumes = Collections.newSetFromMap(new ConcurrentHashMap<>());
  protected final int _numberOfMountSnapshots;
  protected final boolean _nohupProcess;
  protected final File _workingDir;
  protected final HdfsSnapshotStrategy _snapshotStrategy;
  protected final Service _service;
  protected final Timer _umountTimer;
  protected final Timer _cleanupTimer;
  protected final Object _mountLock = new Object();
  protected final Map<String, Long> _idToFileMap = new ConcurrentHashMap<>();

  public BlockPackStorage(BlockPackStorageConfig config) throws IOException, InterruptedException {
    _service = config.getService();
    addServiceExtras(_service);
    _snapshotStrategy = config.getStrategy();
    _nohupProcess = config.isNohupProcess();
    _numberOfMountSnapshots = config.getNumberOfMountSnapshots();
    LastestHdfsSnapshotStrategy.setMaxNumberOfMountSnapshots(_numberOfMountSnapshots);

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

    _configuration = config.getConfiguration();
    FileSystem fileSystem = getFileSystem(config.getRemotePath());
    Path remotePath = config.getRemotePath()
                            .makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    _root = remotePath;

    LOGGER.info("Creating hdfs root path {}", _root);
    getUgi().doAs(HdfsPriv.create(() -> getFileSystem(_root).mkdirs(_root)));

    _localLogDir = config.getLogDir();
    _localLogDir.mkdirs();

    _workingDir = config.getWorkingDir();
    _workingDir.mkdirs();

    long period = TimeUnit.MINUTES.toMillis(1);
    {
      _umountTimer = new Timer("Check Volumes For Umount", true);
      _umountTimer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          try {
            cleanupOldPackProcessMounts();
          } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
          }
        }
      }, period, period);
    }
    {
      _cleanupTimer = new Timer("Remove Old Volume Artifacts", true);
      _cleanupTimer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          try {
            cleanupOldPackProcessArtifacts();
          } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
          }
        }

      }, period, period);
    }
  }

  private void cleanupOldPackProcessMounts() throws IOException {
    List<File> umountedPackProcessMounts = getUmountedPackProcessMounts();
    List<File> shutdownPackProcessList = calculateShutdown(umountedPackProcessMounts);

    for (File volumeDir : shutdownPackProcessList) {
      String id = volumeDir.getName();
      String volumeName = volumeDir.getParentFile()
                                   .getName();

      File localDevice = getLocalDevice(volumeName, id);

      File brick = new File(localDevice, BRICK);
      synchronized (_mountLock) {
        if (brick.exists()) {
          try {
            if (!isDockerStillUsingMount(volumeName)) {
              LOGGER.info("Shutdown pack volume that is not in use {}", volumeName);
              shutdownPack(volumeName, id);
              _idToFileMap.remove(volumeDir.getCanonicalPath());
            }
          } catch (Exception e) {
            LOGGER.error("Unknown error", e);
          }
        }
      }
    }
  }

  private List<File> calculateShutdown(List<File> umountedPackProcessMounts) throws IOException {
    List<File> shutdown = new ArrayList<>();
    for (File file : umountedPackProcessMounts) {
      String key = file.getCanonicalPath();
      Long ts = _idToFileMap.get(key);
      if (ts == null) {
        _idToFileMap.put(key, System.currentTimeMillis());
      } else {
        if (ts + MAX_AGE < System.currentTimeMillis()) {
          shutdown.add(file);
        }
      }
    }
    return shutdown;
  }

  private List<File> getUmountedPackProcessMounts() throws IOException {
    List<File> results = new ArrayList<>();
    File volumesDir = getVolumesDir();
    for (File volumeParentDir : volumesDir.listFiles()) {
      String volumeName = volumeParentDir.getName();
      List<String> ids = getPossibleVolumeIds(volumeName);
      for (String id : ids) {
        File localFileSystemMount = getLocalFileSystemMount(volumeName, id);
        if (!isMounted(localFileSystemMount)) {
          File volumeDir = getVolumeDir(volumeName, id);
          LOGGER.info("unmounted pack process {} {}", volumeName, id);
          results.add(volumeDir);
        }
      }
    }
    return results;
  }

  private void cleanupOldPackProcessArtifacts() {
    // File volumesDir = getVolumesDir();
    // for (File volumeDir : volumesDir.listFiles()) {
    // String volumeName = volumeDir.getName();
    // File localDevice = getLocalDevice(volumeName);
    // synchronized (_mountLock) {
    //
    // File brick = new File(localDevice, BRICK);
    // if (!brick.exists()) {
    // LOGGER.info("Removing old artifacts {}", localDevice);
    // Utils.rmr(localDevice);
    // }
    // }
    // }
  }

  private UserGroupInformation getUgi() throws IOException {
    return Utils.getUserGroupInformation();
  }

  private void addServiceExtras(Service service) {
    service.post(VOLUME_DRIVER_MOUNT_DEVICE, (Route) (request, response) -> {
      PackServer.debugInfo(request);
      MountUnmountRequest mountUnmountRequest = PackServer.read(request, MountUnmountRequest.class);
      try {
        String deviceMountPoint = getUgi().doAs(
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
        getUgi().doAs(HdfsPriv.create(
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
    getUgi().doAs(HdfsPriv.create(() -> createVolume(volumeName, options)));
  }

  @Override
  public void remove(String volumeName) throws Exception {
    getUgi().doAs(HdfsPriv.create(() -> removeVolume(volumeName)));
  }

  @Override
  public String mount(String volumeName, String id) throws Exception {
    return getUgi().doAs((PrivilegedExceptionAction<String>) () -> mountVolume(volumeName, id, false));
  }

  @Override
  public void unmount(String volumeName, String id) throws Exception {
    getUgi().doAs(HdfsPriv.create(() -> umountVolume(volumeName, id, false)));
  }

  private boolean isDockerStillUsingMount(String volumeName) throws IOException {
    return isVolumeInUse("proxy/" + volumeName) || isVolumeInUse(volumeName);
  }

  private boolean isVolumeInUse(String volumeName) throws IOException {
    Result result = Utils.execAsResultQuietly(LOGGER, SUDO, DOCKER, PS, NO_TRUNC, Q,
        "--filter \"volume=" + volumeName + "\"");
    return !result.stdout.toString()
                         .trim()
                         .isEmpty();
  }

  @Override
  public boolean exists(String volumeName) throws Exception {
    return getUgi().doAs((PrivilegedExceptionAction<Boolean>) () -> existsVolume(volumeName));
  }

  @Override
  public String getMountPoint(String volumeName) throws IOException {
    LOGGER.info("get mountPoint volume {}", volumeName);
    List<String> ids = getPossibleVolumeIds(volumeName);
    for (String id : ids) {
      File localFileSystemMount = getLocalFileSystemMount(volumeName, id);
      if (isMounted(localFileSystemMount)) {
        LOGGER.info("volume {} is mounted {}", volumeName, localFileSystemMount);
        return localFileSystemMount.getAbsolutePath();
      }
    }
    return null;
  }

  private List<String> getPossibleVolumeIds(String volumeName) {
    File file = new File(getVolumesDir(), volumeName);
    if (!file.exists()) {
      return ImmutableList.of();
    }
    Builder<String> builder = ImmutableList.builder();
    for (File f : file.listFiles((FileFilter) pathname -> pathname.isDirectory())) {
      builder.add(f.getName());
    }
    return builder.build();
  }

  @Override
  public List<String> listVolumes() throws Exception {
    return getUgi().doAs((PrivilegedExceptionAction<List<String>>) () -> listHdfsVolumes());
  }

  protected List<String> listHdfsVolumes() throws IOException, FileNotFoundException {
    LOGGER.info("list volumes");
    FileSystem fileSystem = getFileSystem(_root);
    FileStatus[] listStatus = fileSystem.listStatus(_root);
    List<String> result = new ArrayList<>();
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      if (HdfsBlockStoreAdmin.hasMetaData(fileSystem, path)) {
        result.add(path.getName());
      }
    }
    return result;
  }

  protected boolean existsVolume(String volumeName) throws IOException {
    LOGGER.info("exists {}", volumeName);
    FileSystem fileSystem = getFileSystem(_root);
    return HdfsBlockStoreAdmin.hasMetaData(fileSystem, getVolumePath(volumeName));
  }

  protected void createVolume(String volumeName, Map<String, Object> options) throws IOException {
    LOGGER.info("create volume {}", volumeName);
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
    Utils.dropVolume(volumePath, fileSystem);
  }

  protected String mountVolume(String volumeName, String id, boolean deviceOnly)
      throws IOException, FileNotFoundException, InterruptedException, KeeperException {
    LOGGER.info("mountVolume {} id {} deviceOnly {}", volumeName, id, deviceOnly);

    if (!existsVolume(volumeName)) {
      createVolume(volumeName, ImmutableMap.of());
    }
    LOGGER.info("Mount Id {} volumeName {}", id, volumeName);

    Path volumePath = getVolumePath(volumeName);
    LOGGER.info("Mount Id {} volumePath {}", id, volumePath);

    File logDir = getLogDir(volumeName);
    LOGGER.info("Mount Id {} logDir {}", id, logDir);
    File localMetrics = getLocalMetrics(logDir);
    LOGGER.info("Mount Id {} localMetrics {}", id, localMetrics);

    File localFileSystemMount = getLocalFileSystemMount(volumeName, id);
    LOGGER.info("Mount Id {} localFileSystemMount {}", id, localFileSystemMount);
    File localDevice = getLocalDevice(volumeName, id);
    LOGGER.info("Mount Id {} localDevice {}", id, localDevice);

    File localCache = getLocalCache(volumeName, id);
    LOGGER.info("Mount Id {} localCache {}", id, localCache);
    File libDir = getLibDir(volumeName, id);
    LOGGER.info("Mount Id {} libDir {}", id, libDir);
    File configFile = getConfigFile(volumeName, id);
    LOGGER.info("Mount Id {} configFile {}", id, configFile);
    File volumeDir = getVolumeDir(volumeName, id);
    LOGGER.info("Mount Id {} volumeDir {}", id, volumeDir);

    FileSystem fileSystem = getFileSystem(volumePath);
    HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(fileSystem, volumePath);
    if (metaData == null) {
      throw new IOException("No metadata found for path " + volumePath);
    }

    synchronized (_mountLock) {

      libDir.mkdirs();
      localCache.mkdirs();
      localFileSystemMount.mkdirs();
      localDevice.mkdirs();
      localMetrics.mkdirs();

      if (isMounted(localFileSystemMount)) {
        LOGGER.info("volume {} id {} already mounted {}", volumeName, id, localFileSystemMount.getCanonicalPath());
        return localFileSystemMount.getCanonicalPath();
      }

      String path = volumePath.toUri()
                              .getPath();

      BlockPackFuseConfigBuilder configBuilder = BlockPackFuseConfig.builder();
      BlockPackFuseConfig config = configBuilder.volumeName(volumeName)
                                                .fuseMountLocation(localDevice.getAbsolutePath())
                                                .fsMetricsLocation(localMetrics.getAbsolutePath())
                                                .fsLocalCache(localCache.getAbsolutePath())
                                                .hdfsVolumePath(path)
                                                .numberOfMountSnapshots(_numberOfMountSnapshots)
                                                .build();
      BlockPackFuseProcessBuilder.startProcess(_nohupProcess, volumeName, volumeDir.getAbsolutePath(),
          logDir.getAbsolutePath(), libDir.getAbsolutePath(), configFile.getAbsolutePath(), config);

      File brick = new File(localDevice, BRICK);
      if (!waitForDevice(brick)) {
        Path lockPath = Utils.getLockPathForVolumeMount(volumePath);
        boolean lock = HdfsLock.isLocked(_configuration, lockPath);
        throw new IOException(
            "Error waiting for device " + brick.getCanonicalPath() + " volume is " + (lock ? LOCKED : UNLOCKED));
      }
    }

    File device = new File(localDevice, FuseFileSystemSingleMount.BRICK);
    if (deviceOnly) {
      return device.getAbsolutePath();
    }
    mkfsIfNeeded(metaData, volumeName, device);
    tryToAssignUuid(metaData, device);
    mountFs(metaData, device, localFileSystemMount);
    waitForMount(localDevice);
    HdfsSnapshotUtil.cleanupOldMountSnapshots(fileSystem, volumePath, _snapshotStrategy);
    return localFileSystemMount.getAbsolutePath();
  }

  private static boolean waitForDevice(File brick) throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      if (brick.exists()) {
        return true;
      }
      LOGGER.info("Waiting for device {}", brick);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
    return false;
  }

  private File getConfigFile(String volumeName, String id) {
    return new File(getVolumeDir(volumeName, id), CONFIG_JSON);
  }

  private void tryToAssignUuid(HdfsMetaData metaData, File device) throws IOException {
    LinuxFileSystem linuxFileSystem = metaData.getFileSystemType()
                                              .getLinuxFileSystem();
    if (linuxFileSystem.isUuidAssignmentSupported()) {
      linuxFileSystem.assignUuid(metaData.getUuid(), device);
    }
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

  private File getLibDir(String volumeName, String id) {
    return new File(getVolumeDir(volumeName, id), "lib");
  }

  private File getVolumesDir() {
    return new File(_workingDir, VOLUMES);
  }

  private File getVolumeDir(String volumeName, String id) {
    return new File(new File(getVolumesDir(), volumeName), id);
  }

  private static boolean isMounted(File localFileSystemMount) throws IOException {
    Result result = Utils.execAsResultQuietly(LOGGER, SUDO, MOUNT);
    return result.stdout.contains(localFileSystemMount.getCanonicalPath());
  }

  private File getLocalCache(String volumeName, String id) {
    return new File(getVolumeDir(volumeName, id), "cache");
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

  public static void waitForMount(File localFileSystemMount) throws InterruptedException, IOException {
    while (!isMounted(localFileSystemMount)) {
      LOGGER.info("Waiting for mount {}", localFileSystemMount);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
  }

  private void umountVolume(String volumeName, String id, boolean deviceOnly)
      throws IOException, InterruptedException, FileNotFoundException, KeeperException {
    LOGGER.info("umountVolume {} id {} deviceOnly {}", volumeName, id, deviceOnly);
    if (!isDockerStillUsingMount(volumeName)) {
      shutdownPack(volumeName, id);
      File volumeDir = getVolumeDir(volumeName, id);
      _idToFileMap.remove(volumeDir.getCanonicalPath());
    }
  }

  private void shutdownPack(String volumeName, String id) throws IOException {
    File localDevice = getLocalDevice(volumeName, id);
    File shutdownFile = new File(localDevice, SHUTDOWN);
    try (OutputStream output = new FileOutputStream(shutdownFile)) {
      output.write(1);
    }
  }

  private File getLocalFileSystemMount(String volumeName, String id) {
    return new File(getVolumeDir(volumeName, id), FS);
  }

  private File getLocalDevice(String volumeName, String id) {
    return new File(getVolumeDir(volumeName, id), DEV);
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
