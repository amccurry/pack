package pack.distributed.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.distributed.storage.hdfs.HdfsBlockGarbageCollector;
import pack.distributed.storage.hdfs.PackHdfsBlockGarbageCollector;
import pack.distributed.storage.http.CompactorServerInfo;
import pack.distributed.storage.http.HttpServer;
import pack.distributed.storage.http.HttpServerConfig;
import pack.distributed.storage.http.PackDao;
import pack.distributed.storage.http.TargetServerInfo;
import pack.distributed.storage.http.Volume;
import pack.distributed.storage.kafka.PackKafkaClientFactory;
import pack.distributed.storage.metrics.ConsoleReporter;
import pack.distributed.storage.metrics.MetricsStorageModule;
import pack.distributed.storage.metrics.PrintStreamFactory;
import pack.distributed.storage.monitor.PackWriteBlockMonitorFactory;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.PackServerStatusManager;
import pack.distributed.storage.status.ServerStatusManager;
import pack.distributed.storage.trace.TraceStorageModule;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;
import pack.iscsi.metrics.MetricsRegistrySingleton;
import pack.iscsi.storage.BaseStorageTargetManager;
import pack.iscsi.storage.utils.PackUtils;

public class PackStorageTargetManager extends BaseStorageTargetManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackStorageTargetManager.class);

  private static final String PACK_HTTP_PORT_DEFAULT = "8642";
  private static final String PACK_HTTP_PORT = "PACK_HTTP_PORT";
  private static final String HDFS = "hdfs";

  private final UserGroupInformation _ugi;
  private final Path _rootPath;
  private final Configuration _conf;
  private final PackKafkaClientFactory _packKafkaClientFactory;
  private final File _cacheDir;
  private final PackWriteBlockMonitorFactory _packWriteBlockMonitorFactory;
  private final long _maxWalSize;
  private final long _maxWalLifeTime;
  private final ZooKeeperClient _zk;
  private final ServerStatusManager _serverStatusManager;
  private final HdfsBlockGarbageCollector _hdfsBlockGarbageCollector;
  private final long _delayBeforeRemoval;
  private final MetricRegistry _registry;

  public PackStorageTargetManager() throws IOException {

    MetricRegistry registry = MetricsRegistrySingleton.getInstance();
    _registry = registry;

    AtomicReference<byte[]> metricsOutput = new AtomicReference<>();
    setupReporter(registry, metricsOutput);

    int httpPort = Integer.parseInt(PackUtils.getEnv(PACK_HTTP_PORT, PACK_HTTP_PORT_DEFAULT));

    PackDao packDao = newPackDao();
    HttpServerConfig httpServerConfig = HttpServerConfig.builder()
                                                        .port(httpPort)
                                                        .textMetricsOutput(metricsOutput)
                                                        .packDao(packDao)
                                                        .build();
    HttpServer.startHttpServer(httpServerConfig);

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

    String kafkaZkConnection = PackConfig.getKafkaZkConnection();
    _packKafkaClientFactory = new PackKafkaClientFactory(kafkaZkConnection);
    _packWriteBlockMonitorFactory = new PackWriteBlockMonitorFactory();

    TargetServerInfo info = getInfo();
    TargetServerInfo.register(_zk, info);
  }

  private TargetServerInfo getInfo() throws IOException {
    List<String> addresses = PackUtils.getEnvListFailIfMissing("PACK_ISCSI_ADDRESS");
    String address = addresses.get(0);
    String bindAddress = address;
    if (address.contains("|")) {
      List<String> list = Splitter.on('|')
                                  .splitToList(address);
      address = list.get(0);
      bindAddress = list.get(1);
    }
    return TargetServerInfo.builder()
                           .address(address)
                           .bindAddress(bindAddress)
                           .hostname(InetAddress.getLocalHost()
                                                .getHostName())
                           .build();
  }

  @Override
  public String getType() {
    return HDFS;
  }

  @Override
  public IStorageModule createNewStorageModule(String name) throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<IStorageModule>) () -> {
        Path volumeDir = new Path(_rootPath, name);
        PackMetaData metaData = getMetaData(volumeDir);
        File cacheDir = new File(_cacheDir, name);
        PackUtils.rmr(cacheDir);
        cacheDir.mkdirs();
        WriteBlockMonitor monitor = _packWriteBlockMonitorFactory.create(name);
        _serverStatusManager.register(name, monitor);
        PackStorageModule module = new PackStorageModule(name, metaData, _conf, volumeDir, _packKafkaClientFactory,
            _ugi, cacheDir, monitor, _serverStatusManager, _hdfsBlockGarbageCollector, _maxWalSize, _maxWalLifeTime);
        IStorageModule storageModule = MetricsStorageModule.wrap(name, _registry, module);
        return TraceStorageModule.traceIfEnabled(storageModule);
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

  private static void setupReporter(MetricRegistry registry, AtomicReference<byte[]> ref) {
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
    ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
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
      public void createVolume(String name, PackMetaData metaData) throws IOException {
        try {
          _ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            metaData.write(_conf, new Path(_rootPath, name));
            return null;
          });
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
  }
}
