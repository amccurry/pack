package pack.block.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
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
import pack.block.fuse.FuseFileSystemSingleMount;
import pack.block.util.Utils;
import pack.zk.utils.ZooKeeperClient;
import pack.zk.utils.ZooKeeperLockManager;

public class BlockPackStorage implements PackStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackStorage.class);

  public static final String MOUNT = "/mount";

  private static final String KILL = "kill";
  private static final String UTF_8 = "UTF-8";
  private static final String DATE_FORMAT = "yyyyMMddkkmmss";
  private static final String METRICS = "metrics";

  protected final Configuration _configuration;
  protected final Path _root;
  protected final UserGroupInformation _ugi;
  protected final File _localFileSystemDir;
  protected final File _localDeviceDir;
  protected final File _localLogDir;
  protected final Set<String> _currentMountedVolumes = Collections.newSetFromMap(new ConcurrentHashMap<>());
  protected final String _zkConnection;
  protected final int _zkTimeout;

  public BlockPackStorage(File localFile, Configuration configuration, Path remotePath, UserGroupInformation ugi,
      String zkConnection, int zkTimeout) throws IOException, InterruptedException {
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
    _localFileSystemDir = new File(localFile, "fs");
    _localFileSystemDir.mkdirs();
    _localDeviceDir = new File(localFile, "devices");
    _localDeviceDir.mkdirs();
    _localLogDir = new File(localFile, "logs");
    _localLogDir.mkdirs();
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
  public String getMountPoint(String volumeName) {
    File localMountFile = getLocalFileSystemMount(volumeName);
    LOGGER.info("Get MountPoint volume {} path {}", volumeName, localMountFile);
    if (!localMountFile.exists()) {
      return null;
    }
    return localMountFile.getAbsolutePath();
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
        HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA;
        LOGGER.info("HdfsMetaData volume {} {}", volumeName, metaData);
        HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, volumePath);
      } else {
        LOGGER.info("Create not created volume {}", volumeName);
      }
    }
  }

  private Path getVolumePath(String volumeName) {
    return new Path(_root, volumeName);
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
    LOGGER.info("Mount Volume {} Id {}", volumeName, id);

    File logDir = getLogDir(volumeName);

    Path volumePath = getVolumePath(volumeName);
    File localFileSystemMount = getLocalFileSystemMount(volumeName);
    File localDevice = getLocalDevice(volumeName);
    File localMetrics = getLocalMetrics(logDir);
    localFileSystemMount.mkdirs();
    localDevice.mkdirs();
    localMetrics.mkdirs();

    String path = volumePath.toUri()
                            .getPath();

    BlockPackFuse.startProcess(localDevice.getAbsolutePath(), localFileSystemMount.getAbsolutePath(),
        localMetrics.getAbsolutePath(), path, _zkConnection, _zkTimeout, volumeName, logDir.getAbsolutePath());
    waitForMount(localDevice);
    return localFileSystemMount.getAbsolutePath();
  }

  private File getLocalMetrics(File logDir) {
    File file = new File(logDir, METRICS);
    file.mkdirs();
    return file;
  }

  private File getLogDir(String volumeName) {
    SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
    String formatStr = format.format(new Date());
    File logDir = new File(new File(_localLogDir, volumeName), formatStr);
    logDir.mkdirs();
    return logDir;
  }

  private void waitForMount(File localDevice) throws InterruptedException, IOException {
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    File fusePid = getDeviceFusePidFile(localDevice);
    for (int i = 0; i < 60; i++) {
      if (fusePid.exists()) {
        return;
      }
      LOGGER.info("Waiting for mount {}", localDevice);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
    throw new IOException("Timeout could not mount " + localDevice);
  }

  private File getDeviceFusePidFile(File localDevice) {
    return new File(localDevice, FuseFileSystemSingleMount.FUSE_PID);
  }

  protected void umountVolume(String volumeName, String id)
      throws IOException, InterruptedException, FileNotFoundException, KeeperException {
    LOGGER.info("Unmount Volume {} Id {}", volumeName, id);
    File localDevice = getLocalDevice(volumeName);
    File fusePid = getDeviceFusePidFile(localDevice);
    killMount(fusePid);
  }

  private void killMount(File fusePid) throws IOException {
    String contents;
    try (InputStream input = new FileInputStream(fusePid)) {
      contents = IOUtils.toString(input, UTF_8);
    }
    String pid = getPid(contents);
    Utils.exec(LOGGER, KILL, pid);
  }

  private String getPid(String contents) throws IOException {
    int indexOf = contents.indexOf('@');
    if (indexOf < 0) {
      throw new IOException("Could not determine pid.");
    }
    return contents.substring(0, indexOf);
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
}
