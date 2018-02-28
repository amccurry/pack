package pack.iscsi;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.jscsi.target.TargetServer;
import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;

import pack.block.blockstore.BlockStore;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImpl;
import pack.iscsi.hdfs.HdfsStorageModule;
import pack.iscsi.storage.DataArchiveManager;
import pack.iscsi.storage.DataSyncManager;
import pack.iscsi.storage.HdfsDataArchiveManager;
import pack.iscsi.storage.PackStorageMetaData;
import pack.iscsi.storage.PackStorageModule;
import pack.iscsi.storage.StorageManager;
import pack.iscsi.storage.concurrent.Executors;
import pack.iscsi.storage.kafka.PackKafkaManager;
import pack.iscsi.storage.utils.IOUtils;

public class IscsiServer implements Closeable {

  private static final String PACK = "pack";

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServer.class);

  private final Map<String, TargetServer> _targetServers = new ConcurrentHashMap<>();
  private final ExecutorService _executorService;
  private final Map<String, Future<Void>> _futures = new ConcurrentHashMap<>();
  private final File _cacheDir;
  private final Configuration _configuration;
  private final TargetManager _iscsiTargetManager;
  private final Path _root;
  private final UserGroupInformation _ugi;
  private final List<String> _brokerServers;
  private final MetricRegistry _registry;
  private final HdfsBlockStoreConfig _hdfsBlockStoreConfig;
  private final boolean _hdfsStorageModule;
  private Timer _timer;
  private String _serialId;

  public IscsiServer(IscsiServerConfig config) throws IOException {
    for (String address : config.getAddresses()) {
      _targetServers.put(address,
          new TargetServer(InetAddress.getByName(address), config.getPort(), config.getIscsiTargetManager()));
    }
    _registry = new MetricRegistry();
    _executorService = Executors.newCachedThreadPool("iscsiserver");
    _cacheDir = config.getCacheDir();
    IOUtils.rmr(_cacheDir);
    _cacheDir.mkdirs();
    _configuration = config.getConfiguration();
    _iscsiTargetManager = config.getIscsiTargetManager();
    _root = config.getRoot();
    _ugi = config.getUgi();
    _serialId = config.getSerialId();
    _brokerServers = config.getBrokerServers();
    _hdfsBlockStoreConfig = config.getHdfsBlockStoreConfig();
    _hdfsStorageModule = config.isHdfsStorageModuleEnabled();
  }

  public void registerTargets() throws IOException, InterruptedException, ExecutionException {
    LOGGER.debug("Registering targets");
    FileSystem fileSystem = _root.getFileSystem(_configuration);
    if (fileSystem.exists(_root)) {
      FileStatus[] listStatus = fileSystem.listStatus(_root);
      for (FileStatus fileStatus : listStatus) {
        Path volumePath = fileStatus.getPath();
        if (fileSystem.isDirectory(volumePath)) {
          String name = volumePath.getName();
          if (!_iscsiTargetManager.isValidTarget(_iscsiTargetManager.getFullName(name))) {
            LOGGER.info("Registering target {}", volumePath);
            IStorageModule storageModule;
            if (_hdfsStorageModule) {
              storageModule = getHdfsStorageModule(volumePath);
            } else {
              storageModule = getPackStorageModule(volumePath, name);
            }
            if (storageModule != null) {
              _iscsiTargetManager.register(name, PACK + " " + name, storageModule);
            }
          }
        }
      }
    }
  }

  private IStorageModule getHdfsStorageModule(Path volumePath) throws IOException {
    BlockStore store = new HdfsBlockStoreImpl(_registry, _cacheDir, volumePath.getFileSystem(_configuration),
        volumePath, _hdfsBlockStoreConfig);
    return new HdfsStorageModule(store);
  }

  public void start() {
    for (Entry<String, TargetServer> e : _targetServers.entrySet()) {
      _futures.put(e.getKey(), _executorService.submit(e.getValue()));
    }
    _timer = new Timer("register volumes", true);
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          registerTargets();
        } catch (Throwable t) {
          LOGGER.error("Unknown error");
        }
      }
    }, TimeUnit.SECONDS.toMillis(10), TimeUnit.SECONDS.toMillis(10));
  }

  public void join() throws InterruptedException, ExecutionException {
    for (Future<Void> future : _futures.values()) {
      future.get();
    }
  }

  @Override
  public void close() throws IOException {
    _timer.purge();
    _timer.cancel();
    for (Future<Void> future : _futures.values()) {
      future.cancel(true);
    }
    _executorService.shutdownNow();
  }

  private PackStorageModule getPackStorageModule(Path volumePath, String name)
      throws InterruptedException, ExecutionException, IOException {
    FileSystem fileSystem = volumePath.getFileSystem(_configuration);
    if (!fileSystem.exists(new Path(volumePath, HdfsBlockStoreAdmin.METADATA))) {
      return null;
    }
    Path blockPath = new Path(volumePath, "block");
    fileSystem.mkdirs(blockPath);
    HdfsMetaData hdfsMetaData = HdfsBlockStoreAdmin.readMetaData(fileSystem, volumePath);
    int blockSize = hdfsMetaData.getFileSystemBlockSize();
    long lengthInBytes = hdfsMetaData.getLength();

    String localWalCachePath = _cacheDir.getAbsolutePath() + "/" + _serialId + "/" + name;
    PackStorageMetaData metaData = PackStorageMetaData.builder()
                                                      .blockSize(blockSize)
                                                      .kafkaPartition(0)
                                                      .kafkaTopic(name)
                                                      .lengthInBytes(lengthInBytes)
                                                      .localWalCachePath(localWalCachePath)
                                                      .walCacheMemorySize(1024 * blockSize)
                                                      .maxOffsetPerWalFile(16 * 1024)
                                                      .lagSyncPollWaitTime(10)
                                                      .hdfsPollTime(TimeUnit.SECONDS.toMillis(10))
                                                      .build();
    String bootstrapServers = Joiner.on(',')
                                    .join(_brokerServers);
    PackKafkaManager kafkaManager = new PackKafkaManager(bootstrapServers, _serialId);
    kafkaManager.createTopicIfMissing(metaData.getKafkaTopic());
    DataArchiveManager dataArchiveManager = new HdfsDataArchiveManager(metaData, _configuration, blockPath, _ugi);
    DataSyncManager dataSyncManager = new DataSyncManager(kafkaManager, metaData, dataArchiveManager);
    StorageManager manager = new StorageManager(metaData, dataSyncManager, dataArchiveManager);
    PackStorageModule packStorageModule = new PackStorageModule(metaData.getLengthInBytes(), manager);
    return packStorageModule;
  }
}
