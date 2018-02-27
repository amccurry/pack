package pack.iscsi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;

public class IscsiServerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerMain.class);

  private static final String PACK = "pack";
  private static final String _2018_02 = "2018-02";

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("a", "address", true, "Listening address.");
    options.addOption("p", "path", true, "Hdfs path.");
    options.addOption("C", "cache", true, "Local cache path.");
    options.addOption("r", "remote", true, "Hdfs ugi remote user.");
    options.addOption("u", "current", false, "Hdfs ugi use current user.");
    options.addOption("c", "conf", true, "Hdfs configuration location.");
    options.addOption("b", "brokers", true, "Kafka brokers e.g. \"broker1:9092,broker2:9092,broker3:9092\"");
    options.addOption("i", "path", true, "Hdfs path.");

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

    String serialid;
    if (cmd.hasOption('i')) {
      serialid = cmd.getOptionValue('i');
      LOGGER.info("serialid {}", serialid);
    } else {
      System.err.println("serialid missing");
      printUsageAndExit(options);
      return;
    }

    String brokerServers = null;
    if (cmd.hasOption('b')) {
      brokerServers = cmd.getOptionValue('b');
      LOGGER.info("brokerServers {}", brokerServers);
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

    Path root = new Path(path);
    File cacheDir = new File(cache);
    TargetManager iscsiTargetManager = new BaseTargetManager(_2018_02, PACK);

    List<String> brokerServersList = null;
    if (brokerServers != null) {
      brokerServersList = Splitter.on(',')
                                  .splitToList(brokerServers);
    }
    HdfsBlockStoreConfig hdfsBlockStoreConfig = HdfsBlockStoreConfig.DEFAULT_CONFIG;
    IscsiServerConfig config = IscsiServerConfig.builder()
                                                .addresses(addresses)
                                                .port(3260)
                                                .ugi(ugi)
                                                .configuration(configuration)
                                                .root(root)
                                                .cacheDir(cacheDir)
                                                .iscsiTargetManager(iscsiTargetManager)
                                                .serialId(serialid)
                                                .brokerServers(brokerServersList)
                                                .hdfsStorageModuleEnabled(false)
                                                .hdfsBlockStoreConfig(hdfsBlockStoreConfig)
                                                .build();
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      runServer(config);
      return null;
    });
  }

  public static void runServer(IscsiServerConfig config) throws IOException, InterruptedException, ExecutionException {
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

}
