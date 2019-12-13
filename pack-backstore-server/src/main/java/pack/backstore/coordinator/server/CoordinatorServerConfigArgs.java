package pack.backstore.coordinator.server;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import pack.backstore.config.ServerConfigArgs;
import pack.backstore.coordinator.server.CoordinatorServerConfig.CoordinatorServerConfigBuilder;

public class CoordinatorServerConfigArgs {

  private static final String ZK_LONG = "zk";
  private static final String ZK_SHORT = "z";

  private static final String ZK_PREFIX_LONG = "zkPrefix";
  private static final String ZK_PREFIX_SHORT = "p";

  public static CoordinatorServerConfig create(String[] args) throws Exception {
    Options options = new Options();

    ServerConfigArgs.addServerOptions(options, CoordinatorServerConfig.builder()
                                                                      .build());

    {
      Option option = new Option(ZK_SHORT, ZK_LONG, true, "ZooKeeper connection string");
      option.setRequired(true);
      options.addOption(option);
    }

    {
      Option option = new Option(ZK_PREFIX_SHORT, ZK_PREFIX_LONG, true,
          "ZooKeeper prefix (default " + CoordinatorServerConfig.ZK_PREFIX_DEFAULT + ")");
      option.setRequired(false);
      options.addOption(option);
    }

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    CoordinatorServerConfigBuilder builder = CoordinatorServerConfig.builder();

    ServerConfigArgs.configureOptions(commandLine, builder);

    if (commandLine.hasOption(ZK_SHORT)) {
      String zkConnection = commandLine.getOptionValue(ZK_SHORT);
      builder.zkConnection(zkConnection);
    }
    if (commandLine.hasOption(ZK_PREFIX_SHORT)) {
      String zkPrefix = commandLine.getOptionValue(ZK_PREFIX_SHORT);
      builder.zkPrefix(zkPrefix);
    }
    return builder.build();
  }

}
