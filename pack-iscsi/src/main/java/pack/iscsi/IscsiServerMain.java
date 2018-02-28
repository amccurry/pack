package pack.iscsi;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
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
    Options options = CliUtils.createHdfsOptions();
    options.addOption("a", "address", true, "Listening address.");
    options.addOption("C", "cache", true, "Local cache path.");
    options.addOption("b", "brokers", true, "Kafka brokers e.g. \"broker1:9092,broker2:9092,broker3:9092\"");
    options.addOption("i", "serial", true, "Serial id for iscsi server.");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    Configuration configuration = CliUtils.getConfig(cmd, options);
    UserGroupInformation ugi = CliUtils.getUGI(cmd, options, configuration);
    Path root = CliUtils.getRootPath(cmd, options);

    Set<String> addresses;
    if (cmd.hasOption('a')) {
      String[] values = cmd.getOptionValues('a');
      addresses = ImmutableSet.copyOf(values);
      LOGGER.info("address {}", addresses);
    } else {
      System.err.println("address missing");
      CliUtils.printUsageAndExit(options);
      return;
    }

    String cache;
    if (cmd.hasOption('C')) {
      cache = cmd.getOptionValue('C');
      LOGGER.info("cache {}", cache);
    } else {
      System.err.println("cache missing");
      CliUtils.printUsageAndExit(options);
      return;
    }

    String serialid;
    if (cmd.hasOption('i')) {
      serialid = cmd.getOptionValue('i');
      LOGGER.info("serialid {}", serialid);
    } else {
      System.err.println("serialid missing");
      CliUtils.printUsageAndExit(options);
      return;
    }

    String brokerServers = null;
    if (cmd.hasOption('b')) {
      brokerServers = cmd.getOptionValue('b');
      LOGGER.info("brokerServers {}", brokerServers);
    }

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

}
