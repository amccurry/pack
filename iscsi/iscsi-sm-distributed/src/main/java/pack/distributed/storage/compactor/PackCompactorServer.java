package pack.distributed.storage.compactor;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.http.CompactorServerInfo;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;
import pack.distributed.storage.zk.ZooKeeperLockManager;
import pack.iscsi.storage.utils.PackUtils;

public class PackCompactorServer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackCompactorServer.class);

  private static final String COMPACTION = "/compaction";

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    PackUtils.setupLog4j();
    UserGroupInformation ugi = PackConfig.getUgi();
    String zkConnectionString = PackConfig.getZooKeeperConnection();
    int sessionTimeout = PackConfig.getZooKeeperSessionTimeout();
    Path hdfsPath = PackConfig.getHdfsTarget();
    Configuration configuration = PackConfig.getConfiguration();
    long maxBlockFileSize = PackConfig.getMaxBlockFileSize();
    double maxObsoleteRatio = PackConfig.getMaxObsoleteRatio();

    AtomicBoolean running = new AtomicBoolean(true);
    ShutdownHookManager.get()
                       .addShutdownHook(() -> running.set(false), Integer.MAX_VALUE);

    try (PackCompactorServer packCompactorServer = new PackCompactorServer(configuration, hdfsPath, zkConnectionString,
        sessionTimeout, maxBlockFileSize, maxObsoleteRatio)) {
      while (running.get()) {
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          try {
            packCompactorServer.executeCompaction();
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
          } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
          }
          return null;
        });
      }
    }
  }

  private final Path _path;
  private final Closer _closer;
  private final ZooKeeperLockManager _compactionLockManager;
  private final String _zkConnectionString;
  private final int _sessionTimeout;
  private final long _maxBlockFileSize;
  private final double _maxObsoleteRatio;
  private final Configuration _configuration;
  private final ZooKeeperClient _zooKeeper;

  public PackCompactorServer(Configuration configuration, Path path, String zkConnectionString, int sessionTimeout,
      long maxBlockFileSize, double maxObsoleteRatio) throws IOException {
    _maxBlockFileSize = maxBlockFileSize;
    _maxObsoleteRatio = maxObsoleteRatio;
    _zkConnectionString = zkConnectionString;
    _sessionTimeout = sessionTimeout;
    _closer = Closer.create();
    PackUtils.closeOnShutdown(_closer);
    _configuration = configuration;
    _path = path;
    _zooKeeper = _closer.register(getZk());
    ZkUtils.mkNodesStr(_zooKeeper, COMPACTION + "/lock");
    InetAddress localHost = InetAddress.getLocalHost();
    CompactorServerInfo.register(_zooKeeper, CompactorServerInfo.builder()
                                                                .address(localHost.getHostAddress())
                                                                .hostname(localHost.getHostName())
                                                                .build());
    _compactionLockManager = new ZooKeeperLockManager(_zkConnectionString, _sessionTimeout, COMPACTION + "/lock");
  }

  private ZooKeeperClient getZk() {
    try {
      return ZkUtils.newZooKeeper(_zkConnectionString, _sessionTimeout);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    _closer.close();
  }

  public void executeCompaction() throws IOException, KeeperException, InterruptedException {
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    FileStatus[] listStatus = fileSystem.listStatus(_path);
    for (FileStatus status : listStatus) {
      executeCompactionVolume(status.getPath());
    }
  }

  private void executeCompactionVolume(Path volumePath) throws IOException, KeeperException, InterruptedException {
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    String lockName = PackBlockFileCompactor.getLockName(volumePath);
    if (_compactionLockManager.tryToLock(lockName)) {
      try {
        try (PackBlockFileCompactor compactor = new PackBlockFileCompactor(fileSystem, volumePath, _maxBlockFileSize,
            _maxObsoleteRatio)) {
          compactor.runCompaction();
        }
      } finally {
        _compactionLockManager.unlock(lockName);
      }
    }
  }

}
