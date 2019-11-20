package pack.volume.server;

import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.volume.server.VolumeServerConfig.VolumeServerConfigBuilder;

public class VolumeServerConfigArgs {

  private static final Logger LOGGER = LoggerFactory.getLogger(VolumeServerConfigArgs.class);

  private static final String ZK_LONG = "zk";
  private static final String ZK_SHORT = "z";
  private static final String ZK_PREFIX_LONG = "zkPrefix";
  private static final String ZK_PREFIX_SHORT = "p";

  public static VolumeServerConfig create(String[] args) throws ParseException {
    Options options = new Options();
    {
      Option option = new Option(ZK_SHORT, ZK_LONG, true, "ZooKeeper connection string");
      option.setRequired(true);
      options.addOption(option);
    }
    {
      Option option = new Option(ZK_PREFIX_SHORT, ZK_PREFIX_LONG, true, "ZooKeeper prefix string");
      option.setRequired(true);
      options.addOption(option);
    }

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    VolumeServerConfigBuilder builder = VolumeServerConfig.builder();

    if (commandLine.hasOption(ZK_SHORT)) {
      String zkConnection = commandLine.getOptionValue(ZK_SHORT);
      RetryPolicy retryPolicy = new RetryForever((int) TimeUnit.SECONDS.toMillis(10));
      CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zkConnection, retryPolicy);
      curatorFramework.getUnhandledErrorListenable()
                      .addListener((message, e) -> {
                        LOGGER.error("Unknown error " + message, e);
                      });
      curatorFramework.getConnectionStateListenable()
                      .addListener((c, newState) -> {
                        LOGGER.info("Connection state {}", newState);
                      });
      curatorFramework.start();
      Runtime.getRuntime()
             .addShutdownHook(new Thread(() -> curatorFramework.close()));
      builder.curatorFramework(curatorFramework);
    }

    if (commandLine.hasOption(ZK_PREFIX_SHORT)) {
      String zkPrefix = commandLine.getOptionValue(ZK_PREFIX_SHORT);
      builder.zkPrefix(zkPrefix);
    }

    return builder.build();
  }

}
