package pack.block.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import pack.PackServer;
import pack.PackServer.Result;
import pack.PackStorage;
import pack.block.BlockStore;
import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.fuse.FuseFS;

public class BlockPackStorage implements PackStorage {

  private static final String UTF_8 = "UTF-8";

  private static final Logger LOG = LoggerFactory.getLogger(BlockPackStorage.class);

  protected final Configuration _configuration;
  protected final Path _root;
  protected final UserGroupInformation _ugi;
  protected final File _localFileSystemDir;
  protected final File _localDeviceDir;
  protected final FuseFS _memfs;

  public BlockPackStorage(File localFile, Configuration configuration, Path remotePath, UserGroupInformation ugi)
      throws IOException, InterruptedException {
    _configuration = configuration;
    FileSystem fileSystem = getFileSystem(remotePath);
    remotePath = remotePath.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    _root = remotePath;
    _ugi = ugi;

    LOG.info("Creating hdfs root {}", _root);
    _ugi.doAs(HdfsPriv.create(() -> getFileSystem(_root).mkdirs(_root)));
    _localFileSystemDir = new File(localFile, "fs");
    _localFileSystemDir.mkdirs();
    _localDeviceDir = new File(localFile, "dev");
    _localDeviceDir.mkdirs();
    _memfs = new FuseFS(_localDeviceDir.getAbsolutePath());
    _memfs.localMount(false);
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
    LOG.info("Get MountPoint volume {} path {}", volumeName, localMountFile);
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
    LOG.info("List Volumes");
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
    LOG.info("exists {}", volumeName);
    FileSystem fileSystem = getFileSystem(_root);
    return fileSystem.exists(getVolumePath(volumeName));
  }

  protected void createVolume(String volumeName, Map<String, Object> options) throws IOException {
    Path volumePath = getVolumePath(volumeName);
    FileSystem fileSystem = getFileSystem(volumePath);
    if (!fileSystem.exists(volumePath)) {
      if (fileSystem.mkdirs(volumePath)) {
        LOG.info("Create volume {}", volumeName);
        int fileSystemBlockSize = HdfsBlockStoreConfig.DEFAULT_CONFIG.getFileSystemBlockSize();
        HdfsMetaData metaData = HdfsMetaData.builder()
                                            .length(
                                                (getDefaultVolumeSize() / fileSystemBlockSize) * fileSystemBlockSize)
                                            .build();
        LOG.info("HdfsMetaData volume {} {}", volumeName, metaData);
        HdfsBlockStore.writeHdfsMetaData(metaData, fileSystem, volumePath);
        _memfs.addBlockStore(new HdfsBlockStore(fileSystem, volumePath));
        File localDevice = getLocalDevice(volumeName);
        // mkfs.ext4
        try {
          Result result = PackServer.exec(Arrays.asList("mkfs.ext4", "-F", localDevice.getAbsolutePath()));
          LOG.info("mkfs.ext4 STDOUT {}", IOUtils.toString(result.output, UTF_8));
          LOG.info("mkfs.ext4 STDERR {}", IOUtils.toString(result.error, UTF_8));
          if (result.exitCode != 0) {
            throw new IOException(IOUtils.toString(result.error, UTF_8));
          }
        } catch (InterruptedException e) {
          throw new IOException(e);
        } finally {
          BlockStore blockStore = _memfs.removeBlockStore(volumeName);
          IOUtils.closeQuietly(blockStore);
        }
      } else {
        LOG.info("Create not created volume {}", volumeName);
      }
    }
  }

  private long getDefaultVolumeSize() {
    return 50L * 1024L * 1024L * 1024L;
  }

  private Path getVolumePath(String volumeName) {
    return new Path(_root, volumeName);
  }

  protected void removeVolume(String volumeName) throws IOException {
    LOG.info("Remove Volume {}", volumeName);
    Path volumePath = getVolumePath(volumeName);

    FileSystem fileSystem = getFileSystem(volumePath);
    fileSystem.delete(volumePath, true);
  }

  protected String mountVolume(String volumeName, String id)
      throws IOException, FileNotFoundException, InterruptedException {
    createVolume(volumeName, ImmutableMap.of());
    LOG.info("Mount Volume {} Id {}", volumeName, id);

    Path volumePath = getVolumePath(volumeName);
    FileSystem fileSystem = getFileSystem(volumePath);
    _memfs.addBlockStore(new HdfsBlockStore(fileSystem, volumePath));

    File localFileSystemMount = getLocalFileSystemMount(volumeName);
    localFileSystemMount.mkdirs();
    File localDevice = getLocalDevice(volumeName);
    // mount
    Result result = PackServer.exec(
        Arrays.asList("mount", localDevice.getAbsolutePath(), localFileSystemMount.getAbsolutePath()));

    LOG.info("mount STDOUT {}", IOUtils.toString(result.output, UTF_8));
    LOG.info("mount STDERR {}", IOUtils.toString(result.error, UTF_8));
    if (result.exitCode != 0) {
      throw new IOException(IOUtils.toString(result.error, UTF_8));
    }
    return localFileSystemMount.getAbsolutePath();
  }

  protected void umountVolume(String volumeName, String id)
      throws IOException, InterruptedException, FileNotFoundException {
    LOG.info("Unmount Volume {} Id {}", volumeName, id);
    File localFileSystemMount = getLocalFileSystemMount(volumeName);
    // umount
    Result result = PackServer.exec(Arrays.asList("umount", "-d", localFileSystemMount.getAbsolutePath()));
    LOG.info("umount STDOUT {}", IOUtils.toString(result.output, UTF_8));
    LOG.info("umount STDERR {}", IOUtils.toString(result.error, UTF_8));
    if (result.exitCode != 0) {
      throw new IOException(IOUtils.toString(result.error, UTF_8));
    }
    BlockStore blockStore = _memfs.removeBlockStore(volumeName);
    IOUtils.closeQuietly(blockStore);
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
