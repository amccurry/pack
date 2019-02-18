package pack.block.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
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
import org.apache.hadoop.fs.ContentSummary;
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

import pack.PackStorage;
import pack.block.blockstore.BlockStoreMetaData;
import pack.block.blockstore.hdfs.CreateVolumeRequest;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.lock.PackLockFactory;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import pack.block.blockstore.hdfs.util.HdfsSnapshotUtil;
import pack.block.blockstore.hdfs.util.LastestHdfsSnapshotStrategy;
import pack.block.fuse.FuseFileSystemSingleMount;
import pack.block.server.fs.LinuxFileSystem;
import pack.block.server.json.BlockPackFuseConfig;
import pack.block.server.json.BlockPackFuseConfig.BlockPackFuseConfigBuilder;
import pack.block.util.FileCounter;
import pack.block.util.Utils;
import pack.docker.PackServer;
import pack.docker.json.Err;
import pack.docker.json.MountUnmountRequest;
import pack.docker.json.PathResponse;
import pack.util.ExecUtil;
import pack.util.LogLevel;
import pack.util.Result;
import spark.Route;
import spark.Service;

public class BlockPackStorage implements PackStorage {

  private static final String WALTMP = ".waltmp";

  private static final String WAL = ".wal";

  private static final String BLOCK = ".block";

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
  private static final long MAX_AGE = TimeUnit.MINUTES.toMillis(20);
  private static final String BASH = "bash";
  private static final String LS = "ls";
  private static final String ERROR = ".error.";
  private static final String SNAPSHOT = "snapshot";
  private static final String RF = "-rf";
  private static final String RM = "rm";

  protected final Configuration _configuration;
  protected final Path _root;
  protected final File _localLogDir;
  protected final Set<String> _currentMountedVolumes = Collections.newSetFromMap(new ConcurrentHashMap<>());
  protected final int _numberOfMountSnapshots;
  protected final File _workingDir;
  protected final HdfsSnapshotStrategy _snapshotStrategy;
  protected final Timer _cleanupTimer;
  protected final Map<String, Long> _cleanUpMap = new ConcurrentHashMap<>();
  protected final Map<String, Long> _shutdownMap = new ConcurrentHashMap<>();
  protected final Map<String, Object> _volumeLocks = new ConcurrentHashMap<>();

  public BlockPackStorage(BlockPackStorageConfig config) throws IOException, InterruptedException {
    addServiceExtras(config.getService());
    _snapshotStrategy = config.getStrategy();
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

    long period = TimeUnit.SECONDS.toMillis(30);
    _cleanupTimer = new Timer("Pack cleanup", true);
    _cleanupTimer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        try {
          cleanup();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
      }
    }, period, period);
  }

  void cleanup() throws IOException, InterruptedException {
    LOGGER.debug("cleanup");
    File volumesDir = getVolumesDir();
    if (!volumesDir.exists()) {
      return;
    }
    for (File volumeDir : volumesDir.listFiles()) {
      cleanup(volumeDir);
    }
  }

  private void cleanup(File volumeDir) throws IOException, InterruptedException {
    if (!volumeDir.exists()) {
      return;
    }
    File[] ids = volumeDir.listFiles();
    for (File idFile : ids) {
      cleanup(volumeDir.getName(), idFile);
    }
  }

  private void cleanup(String volumeName, File idFile) throws IOException, InterruptedException {
    if (!idFile.exists()) {
      return;
    }
    String id = idFile.getName();
    LOGGER.debug("cleanup for volume {} id {}", volumeName, id);
    synchronized (getVolumeLock(volumeName)) {
      if (!isVolumeStillInUse(id)) {
        LOGGER.debug("volume {} id {} is not running", volumeName, id);
        if (shouldPerformCleanup(volumeName, id)) {
          LOGGER.debug("volume {} id {} cleanup", volumeName, id);
          ExecUtil.execAsResult(LOGGER, LogLevel.DEBUG, SUDO, RM, RF, idFile.getCanonicalPath());
          LOGGER.info("delete {} {}", idFile, idFile.delete());
          removeCleanupEntry(volumeName, id);
        } else {
          addCleanupEntry(volumeName, id);
        }
      } else {
        LOGGER.debug("volume {} id {} is still in use", volumeName, id);
      }
    }
  }

  private boolean isVolumeStillInUse(String id) throws IOException {
    Result result = ExecUtil.execAsResult(LOGGER, LogLevel.DEBUG, SUDO, MOUNT);
    if (result.exitCode == 0) {
      return result.stdout.contains(id);
    }
    return false;
  }

  private boolean shouldPerformCleanup(String volumeName, String id) {
    Long ts = _cleanUpMap.get(volumeName + id);
    if (ts == null) {
      return false;
    }
    return ts + MAX_AGE < System.currentTimeMillis();
  }

  private void removeCleanupEntry(String volumeName, String id) {
    LOGGER.info("add cleanup entry volume {} id {}", volumeName, id);
    _cleanUpMap.remove(volumeName + id);
  }

  private void addCleanupEntry(String volumeName, String id) {
    if (_cleanUpMap.putIfAbsent(volumeName + id, System.currentTimeMillis()) == null) {
      LOGGER.info("add cleanup entry volume {} id {}", volumeName, id);
    }
  }

  private UserGroupInformation getUgi() throws IOException {
    return Utils.getUserGroupInformation();
  }

  private void addServiceExtras(Service service) {
    if (service == null) {
      return;
    }
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
    BlockStoreMetaData defaultmetaData = BlockStoreMetaData.DEFAULT_META_DATA;
    BlockStoreMetaData metaData = BlockStoreMetaData.setupOptions(defaultmetaData, options);

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
    LOGGER.debug("Mount Id {} volumeName {}", id, volumeName);

    Path volumePath = getVolumePath(volumeName);
    LOGGER.debug("Mount Id {} volumePath {}", id, volumePath);

    File logDir = getLogDir(volumeName);
    LOGGER.debug("Mount Id {} logDir {}", id, logDir);
    File localMetrics = getLocalMetrics(logDir);
    LOGGER.debug("Mount Id {} localMetrics {}", id, localMetrics);

    File localFileSystemMount = getLocalFileSystemMount(volumeName, id);
    LOGGER.debug("Mount Id {} localFileSystemMount {}", id, localFileSystemMount);
    File localDevice = getLocalDevice(volumeName, id);
    LOGGER.debug("Mount Id {} localDevice {}", id, localDevice);

    File localIndex = getLocalIndex(volumeName, id);
    LOGGER.debug("Mount Id {} localIndex {}", id, localIndex);
    File localCache = getLocalCache(volumeName, id);
    LOGGER.debug("Mount Id {} localCache {}", id, localCache);
    File libDir = getLibDir(volumeName, id);
    LOGGER.debug("Mount Id {} libDir {}", id, libDir);
    File configFile = getConfigFile(volumeName, id);
    LOGGER.debug("Mount Id {} configFile {}", id, configFile);
    File volumeDir = getVolumeDir(volumeName, id);
    LOGGER.debug("Mount Id {} volumeDir {}", id, volumeDir);

    FileSystem fileSystem = getFileSystem(volumePath);
    BlockStoreMetaData metaData = HdfsBlockStoreAdmin.readMetaData(fileSystem, volumePath);
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
        BlockPackFuseProcessBuilder.startProcess(volumeName, volumeDir.getCanonicalPath(), logDir.getCanonicalPath(),
            libDir.getCanonicalPath(), configFile.getCanonicalPath(), _configuration, config, metaData);

        File brick = new File(localDevice, BRICK);
        if (!waitForDevice(brick, true, 60)) {
          Path lockPath = Utils.getLockPathForVolumeMount(volumePath);
          boolean lock = PackLockFactory.isLocked(_configuration, lockPath);
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
          boolean lock = PackLockFactory.isLocked(_configuration, lockPath);
          throw new IOException(
              "Error waiting for device " + brick.getCanonicalPath() + " volume is " + (lock ? LOCKED : UNLOCKED));
        }
      } else {
        LOGGER.info("ref counter {}, ref too high for shutdown", value);
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

  private static boolean waitForDevice(File brick, boolean toExist, int timeInSeconds)
      throws InterruptedException, IOException {
    for (int i = 0; i < timeInSeconds; i++) {
      if (toExist) {
        if (ExecUtil.execReturnExitCode(LOGGER, LogLevel.DEBUG, SUDO, LS, brick.getCanonicalPath()) == 0) {
          return true;
        }
      } else {
        if (ExecUtil.execReturnExitCode(LOGGER, LogLevel.DEBUG, SUDO, LS, brick.getCanonicalPath()) != 0) {
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

  private void tryToAssignUuid(BlockStoreMetaData metaData, File device) throws IOException {
    LinuxFileSystem linuxFileSystem = metaData.getFileSystemType()
                                              .getLinuxFileSystem();
    if (linuxFileSystem.isUuidAssignmentSupported()) {
      linuxFileSystem.assignUuid(metaData.getUuid(), device);
    }
  }

  private void mountFs(BlockStoreMetaData metaData, File device, File localFileSystemMount, String volumeName,
      String id) throws IOException, InterruptedException {
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

  private void mkfsIfNeeded(BlockStoreMetaData metaData, String volumeName, File device) throws IOException {
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
    Result result = ExecUtil.execAsResult(LOGGER, LogLevel.DEBUG, SUDO, MOUNT);
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

  private void shutdownPack(String volumeName, String id) throws IOException, InterruptedException {
    File localDevice = getLocalDevice(volumeName, id);
    File shutdownFile = new File(localDevice, SHUTDOWN);

    Process process = ExecUtil.execAsInteractive(LOGGER, LogLevel.DEBUG, SUDO, BASH);
    try (PrintWriter writer = new PrintWriter(process.getOutputStream())) {
      writer.println("echo 1>" + shutdownFile.getCanonicalPath());
    }
    if (process.waitFor() != 0) {
      throw new IOException("Unknown error on shutdown");
    }
  }

  private void createMountErrorSnapshot(String volumeName, String id)
      throws FileNotFoundException, IOException, InterruptedException {
    File localDevice = getLocalDevice(volumeName, id);
    File snapshotFile = new File(localDevice, SNAPSHOT);
    SimpleDateFormat dateFormat = new SimpleDateFormat(HdfsSnapshotStrategy.YYYYMMDDKKMMSS);
    String format = dateFormat.format(new Date());
    String name = ERROR + format;

    Process process = ExecUtil.execAsInteractive(LOGGER, LogLevel.DEBUG, SUDO, BASH);
    LOGGER.info("Creating mount error snapshot {} for volume {} id {}", name, volumeName, id);
    try (PrintWriter writer = new PrintWriter(process.getOutputStream())) {
      writer.println("echo " + name + ">" + snapshotFile.getCanonicalPath());
    }
    if (process.waitFor() != 0) {
      throw new IOException("Unknown error on shutdown");
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

  public static BlockPackStorageInfo getBlockPackStorageInfo(FileSystem fileSystem, Path root, String volumeName)
      throws IOException {
    Path volumePath = new Path(root, volumeName);
    BlockStoreMetaData blockStoreMetaData = HdfsBlockStoreAdmin.readMetaData(fileSystem, volumePath);

    ContentSummary contentSummary = fileSystem.getContentSummary(volumePath);

    long maxWalSize = 0;
    int numberOfBlockFiles = 0;
    int numberOfWalFiles = 0;
    long hdfsSize = contentSummary.getLength();
    long volumeSize = blockStoreMetaData.getLength();

    int numberOfSnapshots = getNumberOfSnapshots(fileSystem, volumePath);
    long hdfsSizeWithSnapshots = contentSummary.getSnapshotSpaceConsumed();

    Path blockPath = new Path(volumePath, "block");
    FileStatus[] listStatus = fileSystem.listStatus(blockPath);
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      long len = fileStatus.getLen();
      String name = path.getName();
      if (name.endsWith(BLOCK)) {
        numberOfBlockFiles++;
      } else if (name.endsWith(WAL) || name.endsWith(WALTMP)) {
        numberOfWalFiles++;
        if (len > maxWalSize) {
          maxWalSize = len;
        }
      }
    }

    return BlockPackStorageInfo.builder()
                               .maxWalSize(maxWalSize)
                               .name(volumeName)
                               .numberOfBlockFiles(numberOfBlockFiles)
                               .numberOfSnapshots(numberOfSnapshots)
                               .numberOfWalFiles(numberOfWalFiles)
                               .hdfsSize(hdfsSize)
                               .hdfsSizeWithSnapshots(hdfsSizeWithSnapshots)
                               .volumeSize(volumeSize)
                               .build();
  }

  private static int getNumberOfSnapshots(FileSystem fileSystem, Path volumePath) throws IOException {
    Path snapshot = new Path(volumePath, ".snapshot");
    FileStatus[] listStatus = fileSystem.listStatus(snapshot);
    if (listStatus == null) {
      return 0;
    }
    return listStatus.length;
  }
}
