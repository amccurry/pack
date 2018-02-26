package pack.iscsi;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.jscsi.target.TargetServer;
import org.jscsi.target.storage.IStorageModule;
import org.jscsi.target.storage.SynchronizedRandomAccessStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImpl;
import pack.iscsi.storage.DataArchiveManager;
import pack.iscsi.storage.DataSyncManager;
import pack.iscsi.storage.HdfsDataArchiveManager;
import pack.iscsi.storage.PackStorageMetaData;
import pack.iscsi.storage.PackStorageModule;
import pack.iscsi.storage.StorageManager;
import pack.iscsi.storage.kafka.PackKafkaManager;

public class IscsiServer implements Closeable {

  private static final String PACK = "pack";
  private static final String _2018_02 = "2018-02";

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServer.class);

  private final Map<String, TargetServer> _targetServers = new ConcurrentHashMap<>();
  private final ExecutorService _executorService;
  private Map<String, Future<Void>> _futures = new ConcurrentHashMap<>();
  private File _cacheDir;
  private Configuration _configuration;
  private TargetManager _iscsiTargetManager;
  private MetricRegistry _registry;
  private Path _root;
  private UserGroupInformation _ugi;

  private Timer _timer;

  public IscsiServer(IscsiServerConfig config) throws IOException {
    for (String address : config.getAddresses()) {
      _targetServers.put(address,
          new TargetServer(InetAddress.getByName(address), config.getPort(), config.getIscsiTargetManager()));
    }
    _executorService = Executors.newCachedThreadPool();
    _cacheDir = config.getCacheDir();
    _configuration = config.getConfiguration();
    _iscsiTargetManager = config.getIscsiTargetManager();
    _registry = config.getRegistry();
    _root = config.getRoot();
    _ugi = config.getUgi();
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

            int blockSize = 512;
            PackStorageMetaData metaData = PackStorageMetaData.builder()
                                                              .blockSize(blockSize)
                                                              .kafkaPartition(0)
                                                              .kafkaTopic(name)
                                                              .lengthInBytes(10L * blockSize * blockSize * blockSize)
                                                              .localWalCachePath("./cache/" + name)
                                                              .walCacheMemorySize(1024 * blockSize)
                                                              .maxOffsetPerWalFile(16 * 1024)
                                                              .lagSyncPollWaitTime(10)
                                                              .hdfsPollTime(TimeUnit.SECONDS.toMillis(10))
                                                              .build();
            String bootstrapServers = "centos-01:9092,centos-02:9092,centos-03:9092,centos-04:9092";
            String groupId = UUID.randomUUID()
                                 .toString();
            PackKafkaManager kafkaManager = new PackKafkaManager(bootstrapServers, groupId);
            kafkaManager.createTopicIfMissing(metaData.getKafkaTopic());
            DataSyncManager dataSyncManager = new DataSyncManager(kafkaManager, metaData);
            DataArchiveManager dataArchiveManager = new HdfsDataArchiveManager(metaData, _configuration, volumePath,
                _ugi);
            StorageManager manager = new StorageManager(metaData, dataSyncManager, dataArchiveManager);
            PackStorageModule packStorageModule = new PackStorageModule(metaData.getLengthInBytes(), manager);

            // HdfsStorageModule module = new
            // HdfsStorageModule(UgiHdfsBlockStore.wrap(_ugi,
            // getBlockStore(_registry, _configuration, _root, _ugi, _cacheDir,
            // volumePath)));
            _iscsiTargetManager.register(name, PACK + " " + name, packStorageModule);
          }
        }
      }
    }
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

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("a", "address", true, "Listening address.");
    options.addOption("p", "path", true, "Hdfs path.");
    options.addOption("C", "cache", true, "Local cache path.");
    options.addOption("r", "remote", true, "Hdfs ugi remote user.");
    options.addOption("u", "current", false, "Hdfs ugi use current user.");
    options.addOption("c", "conf", true, "Hdfs hdfs configuration location.");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    Set<String> addresses;
    if (cmd.hasOption('a')) {
      String[] values = cmd.getOptionValues('a');
      addresses = ImmutableSet.copyOf(values);
      LOGGER.info("address {}", addresses);
    } else {
      System.err.println("address missing");
      printUsageAndExit(options);
      return;
    }

    String path;
    if (cmd.hasOption('p')) {
      path = cmd.getOptionValue('p');
      LOGGER.info("path {}", path);
    } else {
      System.err.println("path missing");
      printUsageAndExit(options);
      return;
    }

    String cache;
    if (cmd.hasOption('C')) {
      cache = cmd.getOptionValue('C');
      LOGGER.info("cache {}", cache);
    } else {
      System.err.println("cache missing");
      printUsageAndExit(options);
      return;
    }

    String remote = null;
    if (cmd.hasOption('r')) {
      remote = cmd.getOptionValue('r');
      LOGGER.info("remote {}", remote);
    }

    Boolean current = null;
    if (cmd.hasOption('u')) {
      current = true;
      LOGGER.info("current {}", current);
    }

    if (current != null && remote != null) {
      System.err.println("both remote user and current user are not supported together");
      printUsageAndExit(options);
      return;
    }

    String conf = null;
    if (cmd.hasOption('c')) {
      conf = cmd.getOptionValue('c');
      LOGGER.info("conf {}", conf);
    }

    Configuration configuration = new Configuration();
    if (conf != null) {
      File dir = new File(conf);
      if (!dir.exists()) {
        System.err.println("conf dir does not exist");
        printUsageAndExit(options);
        return;
      }
      for (File f : dir.listFiles((FilenameFilter) (dir1, name) -> name.endsWith(".xml"))) {
        if (f.isFile()) {
          configuration.addResource(new FileInputStream(f));
        }
      }
    }

    UserGroupInformation.setConfiguration(configuration);
    UserGroupInformation ugi;
    if (remote != null) {
      ugi = UserGroupInformation.createRemoteUser(remote);
    } else if (current != null) {
      ugi = UserGroupInformation.getCurrentUser();
    } else {
      ugi = UserGroupInformation.getLoginUser();
    }

    MetricRegistry registry = new MetricRegistry();
    Path root = new Path(path);
    File cacheDir = new File(cache);
    TargetManager iscsiTargetManager = new BaseTargetManager(_2018_02, PACK);

    IscsiServerConfig config = IscsiServerConfig.builder()
                                                .addresses(addresses)
                                                .port(3260)
                                                .registry(registry)
                                                .ugi(ugi)
                                                .configuration(configuration)
                                                .root(root)
                                                .cacheDir(cacheDir)
                                                .iscsiTargetManager(iscsiTargetManager)
                                                .build();

    try (IscsiServer iscsiServer = new IscsiServer(config)) {
      iscsiServer.registerTargets();
      iscsiServer.start();
      iscsiServer.join();
    }
  }

  private static void printUsageAndExit(Options options) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(PACK, options);
    System.exit(1);
  }

  private static HdfsBlockStore getBlockStore(MetricRegistry registry, Configuration configuration, Path root,
      UserGroupInformation ugi, File cacheDir, Path volumePath) throws IOException, InterruptedException {
    return ugi.doAs((PrivilegedExceptionAction<HdfsBlockStore>) () -> {
      FileSystem fileSystem = root.getFileSystem(configuration);
      return new HdfsBlockStoreImpl(registry, cacheDir, fileSystem, volumePath);
    });
  }

  private static IStorageModule getStorageModule(File file) throws FileNotFoundException {
    return new SynchronizedRandomAccessStorageModule((file.length() / IStorageModule.VIRTUAL_BLOCK_SIZE), file) {

      @Override
      public void read(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
          Integer commandSequenceNumber) throws IOException {
        LOGGER.info("{} {} read initTag {} comSeqNum {} index {} length {} ", address, port, initiatorTaskTag,
            commandSequenceNumber, storageIndex, bytes.length);
        super.read(bytes, storageIndex);
      }

      @Override
      public void write(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
          Integer commandSequenceNumber, Integer dataSequenceNumber, Integer targetTransferTag) throws IOException {
        LOGGER.info("{} {} write initTag {} comSeqNum {} TargetTransTag {} DataDeqNum {} index {} length {}", address,
            port, initiatorTaskTag, commandSequenceNumber, targetTransferTag, dataSequenceNumber, storageIndex,
            bytes.length);
        super.write(bytes, storageIndex);
      }
    };
  }

}
