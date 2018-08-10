package pack.block.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import pack.block.util.FileCounter;
import pack.block.util.Utils;
import pack.json.Err;
import pack.json.MountUnmountRequest;
import pack.json.PathResponse;
import spark.Route;
import spark.Service;

public class BlockPackStorage implements PackStorage {

  private static final String ERROR = ".error.";

  private static final String SNAPSHOT = "snapshot";

  private static final String LOG = "log";

  private static final String FILTER = "--filter";

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
  private static final String LOCKED = "locked";
  private static final String UNLOCKED = "unlocked";
  private static final String BRICK = "brick";
  private static final String VOLUMES = "volumes";
  private static final String FS = "fs";
  private static final String DEV = "dev";
  private static final String SHUTDOWN = "shutdown";
  private static final String Q = "-q";
  private static final String NO_TRUNC = "--no-trunc";
  private static final String PS = "ps";
  private static final String DOCKER = "docker";
  private static final long MAX_AGE = TimeUnit.MINUTES.toMillis(20);

  protected final Configuration _configuration;
  protected final Path _root;
  protected final File _localLogDir;
  protected final Set<String> _currentMountedVolumes = Collections.newSetFromMap(new ConcurrentHashMap<>());
  protected final int _numberOfMountSnapshots;
  protected final boolean _nohupProcess;
  protected final File _workingDir;
  protected final HdfsSnapshotStrategy _snapshotStrategy;
  protected final Service _service;
  protected final Timer _cleanupTimer;
  protected final Map<String, Long> _cleanUpMap = new ConcurrentHashMap<>();
  protected final Map<String, Long> _shutdownMap = new ConcurrentHashMap<>();
  protected final Map<String, Object> _volumeLocks = new ConcurrentHashMap<>();

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

//    long period = TimeUnit.SECONDS.toMillis(5);
    _cleanupTimer = new Timer("Pack cleanup", true);
//    _cleanupTimer.scheduleAtFixedRate(new TimerTask() {
//      @Override
//      public void run() {
//        try {
//          cleanup();
//        } catch (Throwable t) {
//          LOGGER.error("Unknown error", t);
//        }
//      }
//    }, period, period);
  }

  private void cleanup() throws IOException {
    LOGGER.debug("cleanup");
    File volumesDir = getVolumesDir();
    if (!volumesDir.exists()) {
      return;
    }
    for (File volumeDir : volumesDir.listFiles()) {
      cleanup(volumeDir);
    }
  }

  private void cleanup(File volumeDir) throws IOException {
    if (!volumeDir.exists()) {
      return;
    }
    File[] ids = volumeDir.listFiles();
    for (File idFile : ids) {
      cleanup(volumeDir.getName(), idFile);
    }
  }

  private void cleanup(String volumeName, File idFile) throws IOException {
    if (!idFile.exists()) {
      return;
    }
    String id = idFile.getName();
    File localDevice = getLocalDevice(volumeName, id);
    LOGGER.debug("cleanup for volume {} id {}", volumeName, id);
    synchronized (getVolumeLock(volumeName)) {
      File brick = new File(localDevice, BRICK);
      if (brick.exists()) {
        LOGGER.debug("volume {} id {} still running", volumeName, id);
        if (!isDockerStillUsingMount(volumeName)) {
          LOGGER.debug("volume {} id {} is not in use by docker", volumeName, id);
          if (shouldShutdown(volumeName, id)) {
            shutdownPack(volumeName, id);
            removeShutdownEntry(volumeName, id);
          } else {
            addShutdownEntry(volumeName, id);
          }
        } else {
          LOGGER.debug("volume {} id {} still in use by docker", volumeName, id);
        }
      } else {
        LOGGER.debug("volume {} id {} is not running", volumeName, id);
        if (shouldCleanup(volumeName, id)) {
          LOGGER.debug("volume {} id {} cleanup", volumeName, id);
          for (File file : idFile.listFiles()) {
            if (!file.getName()
                     .equals(LOG)) {
              LOGGER.info("delete -R {}", file);
              Utils.rmr(file);
            } else {
              // This is a symlink, just delete the link not the contents.
              LOGGER.info("delete {} {}", file, file.delete());
            }
          }
          LOGGER.info("delete {} {}", idFile, idFile.delete());
          removeCleanupEntry(volumeName, id);
        } else {
          addCleanupEntry(volumeName, id);
        }
      }
    }
  }

  private boolean shouldCleanup(String volumeName, String id) {
    Long ts = _cleanUpMap.get(volumeName + id);
    if (ts == null) {
      return false;
    }
    return ts + MAX_AGE < System.currentTimeMillis();
  }

  private boolean shouldShutdown(String volumeName, String id) {
    Long ts = _shutdownMap.get(volumeName + id);
    if (ts == null) {
      return false;
    }
    return ts + MAX_AGE < System.currentTimeMillis();
  }

  private void removeCleanupEntry(String volumeName, String id) {
    LOGGER.info("add cleanup entry volume {} id {}", volumeName, id);
    _cleanUpMap.remove(volumeName + id);
  }

  private void removeShutdownEntry(String volumeName, String id) {
    LOGGER.info("remove shutdown entry volume {} id {}", volumeName, id);
    _shutdownMap.remove(volumeName + id);
  }

  private void addCleanupEntry(String volumeName, String id) {
    if (_cleanUpMap.putIfAbsent(volumeName + id, System.currentTimeMillis()) == null) {
      LOGGER.info("add cleanup entry volume {} id {}", volumeName, id);
    }
  }

  private void addShutdownEntry(String volumeName, String id) {
    if (_shutdownMap.putIfAbsent(volumeName + id, System.currentTimeMillis()) == null) {
      LOGGER.info("add shutdown entry volume {} id {}", volumeName, id);
    }
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
    Result result = Utils.execAsResultQuietly(LOGGER, SUDO, DOCKER, PS, NO_TRUNC, Q, FILTER, "volume=" + volumeName);
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
        return localFileSystemMount.getCanonicalPath();
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
    return getUgi().doAs((PrivilegedExceptionAction<List<String>>) () -> listHdfsVolumes(getFileSystem(_root), _root));
  }

  public static List<String> listHdfsVolumes(FileSystem fileSystem, Path root)
      throws IOException, FileNotFoundException {
    LOGGER.info("list volumes");
    FileStatus[] listStatus = fileSystem.listStatus(root);
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

  protected String mountVolume(String volumeName, String id, boolean deviceOnly) throws Exception {
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

    File localIndex = getLocalIndex(volumeName, id);
    LOGGER.info("Mount Id {} localIndex {}", id, localIndex);
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

    FileCounter counter = getFileCounter(volumeName, id);

    try {
      synchronized (getVolumeLock(volumeName)) {
        counter.inc();

        libDir.mkdirs();
        localCache.mkdirs();
        localIndex.mkdirs();
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
                                                  .fuseMountLocation(localDevice.getCanonicalPath())
                                                  .fsMetricsLocation(localMetrics.getCanonicalPath())
                                                  .fsLocalCache(localCache.getCanonicalPath())
                                                  .fsLocalIndex(localIndex.getCanonicalPath())
                                                  .hdfsVolumePath(path)
                                                  .numberOfMountSnapshots(_numberOfMountSnapshots)
                                                  .build();
        BlockPackFuseProcessBuilder.startProcess(_nohupProcess, volumeName, volumeDir.getCanonicalPath(),
            logDir.getCanonicalPath(), libDir.getCanonicalPath(), configFile.getCanonicalPath(), config);

        File brick = new File(localDevice, BRICK);
        if (!waitForDevice(brick, true, 60)) {
          Path lockPath = Utils.getLockPathForVolumeMount(volumePath);
          boolean lock = HdfsLock.isLocked(_configuration, lockPath);
          throw new IOException(
              "Error waiting for device " + brick.getCanonicalPath() + " volume is " + (lock ? LOCKED : UNLOCKED));
        }
      }
      File device = new File(localDevice, FuseFileSystemSingleMount.BRICK);
      if (deviceOnly) {
        return device.getCanonicalPath();
      }
      mkfsIfNeeded(metaData, volumeName, device);
      tryToAssignUuid(metaData, device);
      mountFs(metaData, device, localFileSystemMount, volumeName, id);
      waitForMount(localDevice);
      HdfsSnapshotUtil.cleanupOldMountSnapshots(fileSystem, volumePath, _snapshotStrategy);
      return localFileSystemMount.getCanonicalPath();
    } catch (Exception e) {
      counter.dec();
      throw e;
    }
  }

  private File getLocalIndex(String volumeName, String id) {
    return new File(getVolumeDir(volumeName, id), "index");
  }

  private void umountVolume(String volumeName, String id, boolean deviceOnly) throws Exception {
    LOGGER.info("umountVolume {} id {} deviceOnly {}", volumeName, id, deviceOnly);
    Path volumePath = getVolumePath(volumeName);
    File localDevice = getLocalDevice(volumeName, id);
    synchronized (getVolumeLock(volumeName)) {
      FileCounter counter = getFileCounter(volumeName, id);
      counter.dec();
      long value = counter.getValue();
      if (value == 0) {
        LOGGER.info("ref counter {}, shutdown", value);
        shutdownPack(volumeName, id);
        File brick = new File(localDevice, BRICK);
        if (!waitForDevice(brick, false, 60)) {
          Path lockPath = Utils.getLockPathForVolumeMount(volumePath);
          boolean lock = HdfsLock.isLocked(_configuration, lockPath);
          throw new IOException(
              "Error waiting for device " + brick.getCanonicalPath() + " volume is " + (lock ? LOCKED : UNLOCKED));
        }
      } else {
        LOGGER.info("ref counter {}, wait for shutdown", value);
      }
    }
  }

  private FileCounter getFileCounter(String volumeName, String id) {
    File file = new File(getVolumeDir(volumeName, id), "mount.ref");
    file.getParentFile()
        .mkdirs();
    return new FileCounter(file);
  }

  private synchronized Object getVolumeLock(String volumeName) {
    _volumeLocks.putIfAbsent(volumeName, new Object());
    return _volumeLocks.get(volumeName);
  }

  private static boolean waitForDevice(File brick, boolean toExist, int timeInSeconds) throws InterruptedException {
    for (int i = 0; i < timeInSeconds; i++) {
      if (toExist) {
        if (brick.exists()) {
          return true;
        }
      } else {
        if (!brick.exists()) {
          return true;
        }
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

  private void mountFs(HdfsMetaData metaData, File device, File localFileSystemMount, String volumeName, String id)
      throws IOException {
    String mountOptions = metaData.getMountOptions();
    LinuxFileSystem linuxFileSystem = metaData.getFileSystemType()
                                              .getLinuxFileSystem();
    try {
      linuxFileSystem.mount(device, localFileSystemMount, mountOptions);
    } catch (IOException e) {
      LOGGER.error("Error while trying to mount device {} to {}", device, localFileSystemMount);
      createMountErrorSnapshot(volumeName, id);

      LOGGER.info("Repairing device {}", device);
      linuxFileSystem.repair(device);

      LOGGER.info("Retrying mount device {} to {}", device, localFileSystemMount);
      linuxFileSystem.mount(device, localFileSystemMount, mountOptions);
    }
    if (linuxFileSystem.isFstrimSupported()) {
      linuxFileSystem.fstrim(localFileSystemMount);
    }
    if (linuxFileSystem.isGrowOnlineSupported()) {
      linuxFileSystem.growOnline(localFileSystemMount);
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

  private void shutdownPack(String volumeName, String id) throws IOException {
    File localDevice = getLocalDevice(volumeName, id);
    File shutdownFile = new File(localDevice, SHUTDOWN);
    try (OutputStream output = new FileOutputStream(shutdownFile)) {
      output.write(1);
    }
  }

  private void createMountErrorSnapshot(String volumeName, String id) throws FileNotFoundException, IOException {
    File localDevice = getLocalDevice(volumeName, id);
    File snapshotFile = new File(localDevice, SNAPSHOT);
    SimpleDateFormat dateFormat = new SimpleDateFormat(HdfsSnapshotStrategy.YYYYMMDDKKMMSS);
    try (OutputStream output = new FileOutputStream(snapshotFile)) {
      String format = dateFormat.format(new Date());
      String name = ERROR + format;
      LOGGER.info("Creating mount error snaphost {} for volume {} id {}", name, volumeName, id);
      output.write(name.getBytes());
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
