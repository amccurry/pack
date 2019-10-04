package pack.iscsi.wal.remote;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class RemoteWALServerArgsUtil {

  private static final String PREFIX = "prefix";
  private static final String WALDIR = "waldir";
  private static final String ZK = "zk";
  private static final String SHORT_WAL_DIR = "d";
  private static final String SHORT_ZK_PREFIX = "p";
  private static final String SHORT_OPTION_ZK_CONNECTION = "z";

  public static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();
    {
      Option option = new Option(SHORT_OPTION_ZK_CONNECTION, ZK, true, "ZooKeeper Connection");
      option.setRequired(true);
      options.addOption(option);
    }
    {
      Option option = new Option(SHORT_ZK_PREFIX, PREFIX, true, "ZooKeeper Prefix");
      option.setRequired(true);
      options.addOption(option);
    }
    {
      Option option = new Option(SHORT_WAL_DIR, WALDIR, true, "Log directory");
      option.setRequired(true);
      options.addOption(option);
    }
    CommandLineParser parser = new DefaultParser();
    return parser.parse(options, args);
  }

  public static String getZkConnection(CommandLine cmd) {
    if (cmd.hasOption(ZK)) {
      return cmd.getOptionValue(ZK);
    }
    return null;
  }

  public static String getZkPrefix(CommandLine cmd) {
    if (cmd.hasOption(PREFIX)) {
      return cmd.getOptionValue(PREFIX);
    }
    return null;
  }

  public static File getWalLogDir(CommandLine cmd) {
    if (cmd.hasOption(WALDIR)) {
      return new File(cmd.getOptionValue(WALDIR));
    }
    return null;
  }

}
