package pack.distributed.storage;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.jscsi.target.connection.Connection;
import org.jscsi.target.connection.TargetSession;
import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.distributed.storage.broadcast.PackBroadcastFactory;
import pack.distributed.storage.hdfs.HdfsBlockGarbageCollector;
import pack.distributed.storage.hdfs.PackHdfsBlockGarbageCollector;
import pack.distributed.storage.http.CompactorServerInfo;
import pack.distributed.storage.http.HttpServer;
import pack.distributed.storage.http.HttpServerConfig;
import pack.distributed.storage.http.Metric;
import pack.distributed.storage.http.PackDao;
import pack.distributed.storage.http.Session;
import pack.distributed.storage.http.TargetServerInfo;
import pack.distributed.storage.http.Volume;
import pack.distributed.storage.kafka.PackKafkaBroadcastFactory;
import pack.distributed.storage.kafka.PackKafkaClientFactory;
import pack.distributed.storage.metrics.MetricsStorageModule;
import pack.distributed.storage.metrics.json.JsonReporter;
import pack.distributed.storage.metrics.json.SetupJvmMetrics;
import pack.distributed.storage.metrics.text.PrintStreamFactory;
import pack.distributed.storage.metrics.text.TextReporter;
import pack.distributed.storage.monitor.PackWriteBlockMonitorFactory;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.PackServerStatusManager;
import pack.distributed.storage.status.ServerStatusManager;
import pack.distributed.storage.trace.TraceStorageModule;
import pack.distributed.storage.zk.PackZooKeeperBroadcastFactory;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;
import pack.iscsi.error.NoExceptions;
import pack.iscsi.metrics.MetricsRegistrySingleton;
import pack.iscsi.storage.BaseStorageTargetManager;
import pack.iscsi.storage.utils.PackUtils;

public class PackStorageTargetManager extends BaseStorageTargetManager implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackStorageTargetManager.class);

  private static final String PACK_ISCSI_ADDRESS = "PACK_ISCSI_ADDRESS";
  private static final String CLIENT_ADDRESS = "clientAddress";
  private static final String TARGET_SERVER_ADDRESS = "targetServerAddress";
  private static final String TARGET_SESSIONS = "/target-sessions";
  private static final String HDFS = "hdfs";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final UserGroupInformation _ugi;
  private final Path _rootPath;
  private final Configuration _conf;
  private final File _cacheDir;
  private final long _maxWalSize;
  private final long _maxWalLifeTime;
  private final ZooKeeperClient _zk;
  private final long _delayBeforeRemoval;
  private final MetricRegistry _registry;
  private final JsonReporter _jsonReporter;
  private final String _hostAddress;

  private final PackWriteBlockMonitorFactory _packWriteBlockMonitorFactory;
  private final PackBroadcastFactory _broadcastFactory;
  private final ServerStatusManager _serverStatusManager;
  private final HdfsBlockGarbageCollector _hdfsBlockGarbageCollector;

  public PackStorageTargetManager() throws IOException {
    this(InetAddress.getLocalHost()
                    .getHostAddress());
  }

  public PackStorageTargetManager(String hostAddress) throws IOException {
    _hostAddress = hostAddress;
    MetricRegistry registry = MetricsRegistrySingleton.getInstance();
    _registry = registry;
    _jsonReporter = new JsonReporter(_registry);
    _jsonReporter.start(3, TimeUnit.SECONDS);
    PackUtils.closeOnShutdown(_jsonReporter);

    SetupJvmMetrics.setup(_registry);

    AtomicReference<byte[]> metricsOutput = new AtomicReference<>();
    setupTextReporter(metricsOutput);

    _cacheDir = PackConfig.getWalCachePath();
    _ugi = PackConfig.getUgi();
    _conf = PackConfig.getConfiguration();
    _rootPath = PackConfig.getHdfsTarget();
    _maxWalSize = PackConfig.getMaxWalSize();
    _maxWalLifeTime = PackConfig.getMaxWalLifeTime();
    _delayBeforeRemoval = PackConfig.getHdfsBlockGCDelay();

    String zkConnectionString = PackConfig.getZooKeeperConnection();
    int sessionTimeout = PackConfig.getZooKeeperSessionTimeout();

    String writeBlockMonitorBindAddress = PackConfig.getWriteBlockMonitorBindAddress();
    int writeBlockMonitorPort = PackConfig.getWriteBlockMonitorPort();
    String writeBlockMonitorAddress = PackConfig.getWriteBlockMonitorAddress();

    _zk = ZkUtils.addOnShutdownCloseTrigger(ZkUtils.newZooKeeper(zkConnectionString, sessionTimeout));
    _serverStatusManager = new PackServerStatusManager(_zk, writeBlockMonitorBindAddress, writeBlockMonitorPort,
        writeBlockMonitorAddress);

    PackUtils.closeOnShutdown(_serverStatusManager, _zk);

    _hdfsBlockGarbageCollector = new PackHdfsBlockGarbageCollector(_ugi, _conf, _delayBeforeRemoval);

    // String kafkaZkConnection = PackConfig.getKafkaZkConnection();
    // if (kafkaZkConnection != null) {
    // _broadcastFactory = new PackKafkaBroadcastFactory(new
    // PackKafkaClientFactory(kafkaZkConnection));
    // } else {
    _broadcastFactory = new PackZooKeeperBroadcastFactory(_zk);
    // }

    _packWriteBlockMonitorFactory = new PackWriteBlockMonitorFactory();

    TargetServerInfo info = getInfo();
    TargetServerInfo.register(_zk, info);

    PackDao packDao = newPackDao();

    int httpPort = PackConfig.getHttpPort();
    String httpAddress = PackConfig.getHttpAddress();
    HttpServerConfig httpServerConfig = HttpServerConfig.builder()
                                                        .address(httpAddress)
                                                        .port(httpPort)
                                                        .textMetricsOutput(metricsOutput)
                                                        .packDao(packDao)
                                                        .build();
    HttpServer.startHttpServer(httpServerConfig, _jsonReporter);
  }

  private TargetServerInfo getInfo() throws IOException {
    List<String> addresses = PackUtils.getPropertyListFailIfMissing(PACK_ISCSI_ADDRESS);
    String address = addresses.get(0);
    String bindAddress = address;
    if (address.contains("|")) {
      List<String> list = Splitter.on('|')
                                  .splitToList(address);
      address = list.get(0);
      bindAddress = list.get(1);
    }

    InetAddress bindAddr = InetAddress.getByName(bindAddress);
    InetAddress addr = InetAddress.getByName(address);
    return TargetServerInfo.builder()
                           .address(addr.getHostAddress())
                           .bindAddress(bindAddr.getHostAddress())
                           .hostname(addr.getHostName())
                           .build();
  }

  private static byte[] getDataForTargetLock(String targetServerAddress, String clientAddress) throws IOException {
    Map<String, String> map = new TreeMap<>();
    map.put(TARGET_SERVER_ADDRESS, targetServerAddress);
    map.put(CLIENT_ADDRESS, clientAddress);
    return MAPPER.writeValueAsBytes(map);
  }

  @Override
  public String getType() {
    return HDFS;
  }

  @Override
  public IStorageModule createNewStorageModule(String targetName) throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<IStorageModule>) () -> {
        Path volumeDir = new Path(_rootPath, targetName);
        PackMetaData metaData = getMetaData(volumeDir);
        File cacheDir = new File(_cacheDir, targetName);
        PackUtils.rmr(cacheDir);
        cacheDir.mkdirs();
        WriteBlockMonitor monitor = _packWriteBlockMonitorFactory.create(targetName);
        _serverStatusManager.register(targetName, monitor);
        PackStorageModule module = new PackStorageModule(targetName, metaData, _conf, volumeDir, _broadcastFactory,
            _ugi, cacheDir, monitor, _serverStatusManager, _hdfsBlockGarbageCollector, _maxWalSize, _maxWalLifeTime,
            _zk, _hostAddress);
        IStorageModule storageModule = MetricsStorageModule.wrap(targetName, _registry, module);
        return NoExceptions.retryForever(TraceStorageModule.traceIfEnabled(storageModule));
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private PackMetaData getMetaData(Path volumeDir) throws IOException {
    FileSystem fileSystem = volumeDir.getFileSystem(_conf);
    if (!fileSystem.exists(volumeDir)) {
      throw new FileNotFoundException("Volume path " + volumeDir + " not found");
    }
    return PackMetaData.read(_conf, volumeDir);
  }

  @Override
  public List<String> getVolumeNames() {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<List<String>>) () -> {
        Builder<String> builder = ImmutableList.builder();
        FileSystem fileSystem = _rootPath.getFileSystem(_conf);
        if (!fileSystem.exists(_rootPath)) {
          return builder.build();
        }
        FileStatus[] listStatus = fileSystem.listStatus(_rootPath);
        for (FileStatus fileStatus : listStatus) {
          if (fileStatus.isDirectory()) {
            Path path = fileStatus.getPath();
            builder.add(path.getName());
          }
        }
        return builder.build();
      });
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void setupTextReporter(AtomicReference<byte[]> ref) {
    PrintStreamFactory printStreamFactory = () -> {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      return new PrintStream(outputStream) {
        @Override
        public void close() {
          super.close();
          ref.set(outputStream.toByteArray());
        }
      };
    };
    TextReporter reporter = TextReporter.forRegistry(_registry)
                                        .convertRatesTo(TimeUnit.SECONDS)
                                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                                        .outputTo(printStreamFactory)
                                        .build();
    reporter.start(3, TimeUnit.SECONDS);

    PackUtils.closeOnShutdown(reporter);
  }

  private PackDao newPackDao() {
    return new PackDao() {
      @Override
      public List<Volume> getVolumes() {
        try {
          return _ugi.doAs(new PrivilegedExceptionAction<List<Volume>>() {
            @Override
            public List<Volume> run() throws Exception {
              FileSystem fileSystem = _rootPath.getFileSystem(_conf);
              FileStatus[] listStatus = fileSystem.listStatus(_rootPath);
              Builder<Volume> builder = ImmutableList.builder();
              for (FileStatus fileStatus : listStatus) {
                if (fileStatus.isDirectory()) {
                  Path path = fileStatus.getPath();
                  try {
                    PackMetaData metaData = getMetaData(path);
                    String name = path.getName();
                    Volume volume = Volume.builder()
                                          .hdfsPath(path.toString())
                                          .iqn(getFullName(name))
                                          .kafkaTopic(metaData.getTopicId())
                                          .name(name)
                                          .size(metaData.getLength())
                                          .serialId(metaData.getSerialId())
                                          .build();
                    builder.add(volume);
                  } catch (FileNotFoundException e) {
                    LOGGER.error("Path not not valid volume " + path);
                  }
                }
              }
              return builder.build();
            }
          });
        } catch (IOException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public List<TargetServerInfo> getTargets() {
        return TargetServerInfo.list(_zk);
      }

      @Override
      public List<CompactorServerInfo> getCompactors() {
        return CompactorServerInfo.list(_zk);
      }

      @Override
      public List<Metric> getMetrics() {
        return _jsonReporter.getMetricRef();
      }

      @Override
      public List<Session> getSessions() {
        try {
          return getCurrentSessions();
        } catch (KeeperException | InterruptedException | IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  protected List<Session> getCurrentSessions() throws KeeperException, InterruptedException, IOException {
    Stat stat = _zk.exists(TARGET_SESSIONS, false);
    if (stat == null) {
      return ImmutableList.of();
    }
    while (true) {
      try {
        List<Session> sessions = new ArrayList<>();
        List<String> targets = _zk.getChildren(TARGET_SESSIONS, false);
        for (String target : targets) {
          String targetPath = TARGET_SESSIONS + "/" + target;
          Stat targetStat = _zk.exists(targetPath, false);
          if (targetStat == null) {
            continue;
          }
          byte[] lockAddress = _zk.getData(targetPath, false, targetStat);
          List<String> locks = _zk.getChildren(targetPath, false);
          for (String lock : locks) {
            String path = targetPath + "/" + lock;
            Stat lockStat = _zk.exists(path, false);
            if (lockStat != null) {
              byte[] data = _zk.getData(path, false, lockStat);
              Map<String, String> map = MAPPER.readValue(data, Map.class);
              sessions.add(Session.builder()
                                  .clientAddress(map.get(CLIENT_ADDRESS))
                                  .targetServerAddress(map.get(TARGET_SERVER_ADDRESS))
                                  .writeLockClientAddress(new String(lockAddress))
                                  .iqn(getFullName(target))
                                  .build());
            }
          }
        }
        Collections.sort(sessions);
        return sessions;
      } catch (KeeperException e) {
        if (e.code() == Code.NONODE) {
          continue;
        }
        throw e;
      }
    }
  }

  public static boolean checkSessionWritablity(ZooKeeperClient zk, String targetHostAddress, String targetName)
      throws IOException, KeeperException, InterruptedException {
    if (isSessionWritable()) {
      return true;
    }
    TargetSession session = getSession();
    if (session == null) {
      throw new RuntimeException("TargetSession missing.");
    }
    Connection connection = session.getConnection();
    if (connection == null) {
      throw new RuntimeException("Connection missing.");
    }
    SocketChannel socketChannel = connection.getSocketChannel();
    if (socketChannel == null) {
      throw new RuntimeException("SocketChannel missing.");
    }
    InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
    String hostAddress = remoteAddress.getAddress()
                                      .getHostAddress();

    String path = TARGET_SESSIONS + "/" + targetName;
    ZkUtils.mkNodesStr(zk, path);

    LOGGER.info("Trying to lock zk path {} to validate session", path);

    String newPath = zk.create(path + "/lock_", getDataForTargetLock(targetHostAddress, hostAddress),
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

    Closeable closeable = (Closeable) () -> {
      try {
        zk.delete(newPath, -1);
      } catch (InterruptedException | KeeperException e) {
        throw new IOException(e);
      }
    };

    List<String> list = new ArrayList<>(zk.getChildren(path, false));
    Collections.sort(list);

    String lockPath = path + "/" + list.get(0);

    LOGGER.info("Lock zk path {}", lockPath);

    if (lockPath.equals(newPath)) {
      // we got the lock
      LOGGER.info("Lock obtained for {}", targetName);
      zk.setData(path, hostAddress.getBytes(), -1);
      getCloser().register(closeable);
      setSessionWritable(true);
      return true;
    } else {
      // we didn't get the lock
      Stat lockStat = zk.exists(lockPath, false);
      long lockMzxid = lockStat.getMzxid();

      LOGGER.info("Lock not obtained for {}, waiting for data in path {} to be updated, lockpath {}", targetName, path,
          lockPath);

      // wait until data version after lock version?
      Stat pathStat = null;
      boolean dataUpdated = false;
      for (int i = 0; i < 10; i++) {
        pathStat = zk.exists(path, false);
        if (pathStat.getMzxid() > lockMzxid) {
          dataUpdated = true;
          break;
        }
        LOGGER.info("Waiting for data in path {} to be updated", path);
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      }

      if (!dataUpdated) {
        LOGGER.error("Data was not updated, failing {}", targetName);
        PackUtils.close(LOGGER, closeable);
        return false;
      }
      // check that the node data is a match to the host name
      byte[] data = zk.getData(path, false, pathStat);
      String hostAddressHoldingLock = new String(data);
      if (hostAddress.equals(hostAddressHoldingLock)) {
        LOGGER.info("Host names match {}", hostAddress);
        getCloser().register(closeable);
        setSessionWritable(true);
        return true;
      } else {
        PackUtils.close(LOGGER, closeable);
        return false;
      }
    }
  }

  @Override
  public void close() throws IOException {
    PackUtils.close(LOGGER, _serverStatusManager, _hdfsBlockGarbageCollector);
  }
}
