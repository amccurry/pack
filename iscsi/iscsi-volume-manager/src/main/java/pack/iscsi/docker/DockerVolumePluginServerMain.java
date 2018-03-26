package pack.iscsi.docker;

import static pack.iscsi.docker.Utils.getEnv;
import static pack.iscsi.docker.Utils.getIqn;
import static pack.iscsi.docker.Utils.iscsiDeleteSession;
import static pack.iscsi.docker.Utils.iscsiDiscovery;
import static pack.iscsi.docker.Utils.iscsiLoginSession;
import static pack.iscsi.docker.Utils.iscsiLogoutSession;
import static pack.iscsi.docker.Utils.waitUntilBlockDeviceIsOnline;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
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

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.http.TargetServerInfo;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;
import pack.iscsi.docker.Utils.Result;
import pack.iscsi.storage.utils.PackUtils;

public class DockerVolumePluginServerMain {

  private static final String RUN_DOCKER_PLUGINS_PACK_SOCK = "/var/lib/pack/pack.sock";
  private static final String DOCKER_PLUGIN_SOCK_PATH = "DOCKER_PLUGIN_SOCK_PATH";
  private static final String ISCSI_ROOT_MOUNT = "ISCSI_ROOT_MOUNT";

  public static void main(String[] args) throws Exception {
    PackUtils.setupLog4j();
    String sockerFile = PackUtils.getProperty(DOCKER_PLUGIN_SOCK_PATH, RUN_DOCKER_PLUGINS_PACK_SOCK);
    DockerVolumePluginServer dockerVolumePluginServer = new DockerVolumePluginServer(true, sockerFile) {
      @Override
      protected VolumeStorageControl getPackStorage() throws Exception {
        return new IscsiVolumeStorageControl();
      }
    };
    dockerVolumePluginServer.runServer();
  }

  static class IscsiVolumeStorageControl implements VolumeStorageControl {

    private static final Logger LOGGER = LoggerFactory.getLogger(IscsiVolumeStorageControl.class);

    private final Configuration _configuration;
    private final UserGroupInformation _ugi;
    private final Path _hdfsTarget;
    private final String _rootMount;
    private final String _zkConn;
    private final int _zkTimeout;
    private final Object _lock = new Object();

    public IscsiVolumeStorageControl() throws IOException {
      _configuration = PackConfig.getConfiguration();
      _ugi = PackConfig.getUgi();
      _hdfsTarget = PackConfig.getHdfsTarget();
      _rootMount = getEnv(ISCSI_ROOT_MOUNT);
      _zkConn = PackConfig.getZooKeeperConnection();
      _zkTimeout = PackConfig.getZooKeeperSessionTimeout();
    }

    @Override
    public void create(String volumeName, Map<String, Object> options) throws Exception {
      PackMetaData metaData = createVolume(volumeName);
      String dev = metaData.getSerialId();
      String iqn = getIqn(volumeName);
      List<TargetServerInfo> targetServers = getTargetServers();

      try {
        synchronized (_lock) {
          iscsiDiscovery(targetServers);
          iscsiLoginSession(iqn);
        }
        String devicePath = "/dev/mapper/" + dev;
        waitUntilFileExists(devicePath);
        Result fsFormatResult = Utils.execAsResult(LOGGER, "sudo", "mkfs.xfs", devicePath);
        if (fsFormatResult.exitCode != 0) {
          throw new Exception("Format failed " + volumeName + " @ " + devicePath);
        }
      } finally {
        synchronized (_lock) {
          iscsiLogoutSession(iqn);
          iscsiDeleteSession(iqn);
        }
      }
    }

    @Override
    public void remove(String volumeName) throws Exception {
      throw new Exception("Not supported yet.");
    }

    @Override
    public String mount(String volumeName, String id) throws Exception {
      String iqn = getIqn(volumeName);
      List<TargetServerInfo> targetServers = getTargetServers();

      PackMetaData metaData = getPackMetaData(volumeName);
      String dev = metaData.getSerialId();
      synchronized (_lock) {
        iscsiDiscovery(targetServers);
        iscsiLoginSession(iqn);
      }
      waitUntilBlockDeviceIsOnline("/dev/mapper/" + dev);
      String mountPoint = getMountPoint(volumeName);
      File mountFile = new File(mountPoint);
      mountFile.mkdirs();
      if (!mountFile.exists()) {
        throw new Exception("Mount point does not exist " + mountFile);
      }
      Result result = Utils.execAsResult(LOGGER, "sudo", "mount", "-o", "async,noatime", "/dev/mapper/" + dev,
          mountFile.getAbsolutePath());
      if (result.exitCode == 0) {
        return mountFile.getAbsolutePath();
      } else {
        throw new Exception(result.stderr);
      }
    }

    @Override
    public String getMountPoint(String volumeName) throws Exception {
      return _rootMount + "/" + volumeName;
    }

    @Override
    public void unmount(String volumeName, String id) throws Exception {
      String mountPoint = getMountPoint(volumeName);
      Result result = Utils.execAsResult(LOGGER, "sudo", "umount", mountPoint);
      if (result.exitCode != 0) {
        throw new Exception(result.stderr);
      }
      String iqn = getIqn(volumeName);
      synchronized (_lock) {
        iscsiLogoutSession(iqn);
        iscsiDeleteSession(iqn);
      }
      PackUtils.rmr(new File(mountPoint));
    }

    @Override
    public List<String> listVolumes() throws Exception {
      return getVolumes();
    }

    @Override
    public boolean exists(String volumeName) throws Exception {
      return listVolumes().contains(volumeName);
    }

    private List<TargetServerInfo> getTargetServers() throws IOException {
      List<TargetServerInfo> list;
      try (ZooKeeperClient zk = ZkUtils.newZooKeeper(_zkConn, _zkTimeout)) {
        list = TargetServerInfo.list(zk);
      }
      return list;
    }

    private PackMetaData getPackMetaData(String volumeName) throws IOException, InterruptedException {
      return _ugi.doAs((PrivilegedExceptionAction<PackMetaData>) () -> PackMetaData.read(_configuration,
          new Path(_hdfsTarget, volumeName)));
    }

    private PackMetaData createVolume(String volumeName) throws IOException, InterruptedException {
      return _ugi.doAs(new PrivilegedExceptionAction<PackMetaData>() {
        @Override
        public PackMetaData run() throws Exception {
          String serialId = PackUtils.generateSerialId()
                                     .toString();
          String newTopicId = PackUtils.getTopic(volumeName, serialId);
          PackMetaData metaData = PackMetaData.builder()
                                              .serialId(serialId)
                                              .topicId(newTopicId)
                                              .build();
          metaData.write(_configuration, new Path(_hdfsTarget, volumeName));
          return metaData;
        }
      });
    }

    private List<String> getVolumes() throws IOException, InterruptedException {
      return _ugi.doAs(new PrivilegedExceptionAction<List<String>>() {
        @Override
        public List<String> run() throws Exception {
          FileSystem fileSystem = _hdfsTarget.getFileSystem(_configuration);
          FileStatus[] listStatus = fileSystem.listStatus(_hdfsTarget);
          Builder<String> builder = ImmutableList.builder();
          for (FileStatus fileStatus : listStatus) {
            Path volume = fileStatus.getPath();
            PackMetaData metaData = PackMetaData.read(_configuration, volume);
            if (metaData != null) {
              builder.add(volume.getName());
            }
          }
          return builder.build();
        }
      });
    }

    private void waitUntilFileExists(String devicePath) throws InterruptedException {
      File file = new File(devicePath);
      for (int i = 0; i < 10; i++) {
        if (file.exists()) {
          return;
        }
        LOGGER.info("Waiting for device {} to be ready", devicePath);
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
      }
    }
  }

}
