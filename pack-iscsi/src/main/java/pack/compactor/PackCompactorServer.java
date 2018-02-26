package pack.compactor;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.io.Closer;

import pack.block.blockstore.compactor.BlockFileCompactor;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.server.BlockPackFuse;
import pack.block.util.Utils;
import pack.zk.utils.ZkUtils;
import pack.zk.utils.ZooKeeperClient;
import pack.zk.utils.ZooKeeperLockManager;

public class PackCompactorServer implements Closeable {

  private static final String PACK = "pack";

  private static final Logger LOGGER = LoggerFactory.getLogger(PackCompactorServer.class);

  private static final String COMPACTION = "/compaction";

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException, ParseException {
    Utils.setupLog4j();

    Options options = new Options();
    options.addOption("p", "path", true, "Hdfs path.");
    options.addOption("r", "remote", true, "Hdfs ugi remote user.");
    options.addOption("u", "current", false, "Hdfs ugi use current user.");
    options.addOption("c", "conf", true, "Hdfs configuration location.");
    options.addOption("C", "cache", true, "Local cache path.");
    options.addOption("t", "zktimeout", true, "ZooKeeper timeout.");
    options.addOption("z", "zkcon", true, "ZooKeeper connection.");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
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

    String zkConn;
    if (cmd.hasOption('z')) {
      zkConn = cmd.getOptionValue('z');
      LOGGER.info("zkConn {}", zkConn);
    } else {
      System.err.println("zkConn missing");
      printUsageAndExit(options);
      return;
    }

    String zkTimeout;
    if (cmd.hasOption('t')) {
      zkTimeout = cmd.getOptionValue('t');
      LOGGER.info("zkTimeout {}", zkTimeout);
    } else {
      zkTimeout = "30000";
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

    File cacheDir = new File(cache);
    cacheDir.mkdirs();
    AtomicBoolean running = new AtomicBoolean(true);
    ShutdownHookManager.get()
                       .addShutdownHook(() -> running.set(false), Integer.MAX_VALUE);

    FileSystem fileSystem = FileSystem.get(configuration);

    List<Path> pathList = getPathList(fileSystem, path);

    try (PackCompactorServer packCompactorServer = new PackCompactorServer(cacheDir, fileSystem, pathList, zkConn,
        Integer.parseInt(zkTimeout))) {
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

  private final FileSystem _fileSystem;
  private final List<Path> _pathList;
  private final Closer _closer;
  private final ZooKeeperLockManager _compactionLockManager;
  private final ZooKeeperLockManager _mountLockManager;
  private final File _cacheDir;
  private final String _zkConnectionString;
  private final int _sessionTimeout;

  public PackCompactorServer(File cacheDir, FileSystem fileSystem, List<Path> pathList, String zkConnectionString,
      int sessionTimeout) throws IOException {
    // coord with zookeeper
    // use zookeeper to know if the block store is mount (to know whether
    // cleanup can be done)
    _zkConnectionString = zkConnectionString;
    _sessionTimeout = sessionTimeout;
    _cacheDir = cacheDir;
    _closer = Closer.create();
    _fileSystem = fileSystem;
    _pathList = pathList;
    try (ZooKeeperClient zooKeeper = getZk()) {
      ZkUtils.mkNodesStr(zooKeeper, COMPACTION + "/lock");
    }
    _mountLockManager = BlockPackFuse.createLockmanager(_zkConnectionString, _sessionTimeout);
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
    for (Path path : _pathList) {
      executeCompaction(path);
    }
  }

  public void executeCompaction(Path root) throws IOException, KeeperException, InterruptedException {
    FileStatus[] listStatus = _fileSystem.listStatus(root);
    for (FileStatus status : listStatus) {
      executeCompactionVolume(status.getPath());
    }
  }

  private void executeCompactionVolume(Path volumePath) throws IOException, KeeperException, InterruptedException {
    String lockName = Utils.getLockName(volumePath);
    if (_compactionLockManager.tryToLock(lockName)) {
      try {
        HdfsMetaData metaData = HdfsBlockStoreAdmin.readMetaData(_fileSystem, volumePath);
        try (BlockFileCompactor compactor = new BlockFileCompactor(_cacheDir, _fileSystem, volumePath, metaData,
            _mountLockManager)) {
          compactor.runCompaction();
        }
      } finally {
        _compactionLockManager.unlock(lockName);
      }
    }
  }

  private static List<Path> getPathList(FileSystem fileSystem, String packRootPathList) throws IOException {
    List<String> list = Splitter.on(',')
                                .splitToList(packRootPathList);
    List<Path> pathList = new ArrayList<>();
    for (String p : list) {
      FileStatus fileStatus = fileSystem.getFileStatus(new Path(p));
      pathList.add(fileStatus.getPath());
    }
    return pathList;
  }

  private static void printUsageAndExit(Options options) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(PACK, options);
    System.exit(1);
  }
}
