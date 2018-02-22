package pack.iscsi;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.UgiHdfsBlockStore;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImpl;
import pack.iscsi.hdfs.HdfsStorageModule;

public class IscsiServer implements Closeable {

  private static final String PACK = "pack";

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServer.class);

  private final TargetServer _targetServer;
  private final ExecutorService _executorService;
  private Future<Void> _future;
  private File _cacheDir;
  private Configuration _configuration;
  private TargetManager _iscsiTargetManager;
  private MetricRegistry _registry;
  private Path _root;
  private UserGroupInformation _ugi;

  private Timer _timer;

  public IscsiServer(IscsiServerConfig config) throws IOException {
    _targetServer = new TargetServer(InetAddress.getByName(config.getAddress()), config.getPort(),
        config.getIscsiTargetManager());
    _executorService = Executors.newSingleThreadExecutor();
    _cacheDir = config.getCacheDir();
    _configuration = config.getConfiguration();
    _iscsiTargetManager = config.getIscsiTargetManager();
    _registry = config.getRegistry();
    _root = config.getRoot();
    _ugi = config.getUgi();
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

  public void registerTargets() throws IOException, InterruptedException {
    LOGGER.info("Registering targets");
    FileSystem fileSystem = _root.getFileSystem(_configuration);
    if (fileSystem.exists(_root)) {
      FileStatus[] listStatus = fileSystem.listStatus(_root);
      for (FileStatus fileStatus : listStatus) {
        Path volumePath = fileStatus.getPath();
        if (fileSystem.isDirectory(volumePath)) {
          String name = volumePath.getName();
          if (!_iscsiTargetManager.isValidTarget(_iscsiTargetManager.getFullName(name))) {
            LOGGER.info("Registering target {}", volumePath);
            HdfsStorageModule module = new HdfsStorageModule(UgiHdfsBlockStore.wrap(_ugi,
                getBlockStore(_registry, _configuration, _root, _ugi, _cacheDir, volumePath)));
            // IStorageModule storageModule = getStorageModule(new
            // File("./storage/test1"));
            _iscsiTargetManager.register(name, PACK + " " + name, module);
          }
        }
      }
    }
  }

  public void start() {
    _future = _executorService.submit(_targetServer);
  }

  public void join() throws InterruptedException, ExecutionException {
    _future.get();
  }

  @Override
  public void close() throws IOException {
    _timer.purge();
    _timer.cancel();
    _future.cancel(true);
    _executorService.shutdownNow();
  }

  public static void main(String[] args) throws Exception {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(PACK);
    String address = "192.168.56.1";
    String date = "2018-02";
    String domain = "com.fortitudetec";

    MetricRegistry registry = new MetricRegistry();
    Configuration configuration = new Configuration();
    Path root = new Path("/pack");
    File cacheDir = new File("./cache");
    TargetManager iscsiTargetManager = new BaseTargetManager(date, domain);

    IscsiServerConfig config = IscsiServerConfig.builder()
                                                .address(address)
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
