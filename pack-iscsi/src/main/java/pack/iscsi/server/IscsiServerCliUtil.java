package pack.iscsi.server;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class IscsiServerCliUtil {
  
  private static final String IP = "ip";
  private static final String PORT = "port";
  private static final String CONFIG = "config";

  private static final String SHORT_IP_OPTION = "I";
  private static final String SHORT_PORT_OPTION = "p";
  private static final String SHORT_CONFIG_OPTION = "C";

  private static final String DEFAULT_IP = "127.0.0.127";
  private static final int DEFAULT_PORT = 3260;
  private static final String DEFAULT_CONFIG_LOCATION = "/etc/pack/conf.d/";

  public static int getPort(CommandLine cmd) {
    if (cmd.hasOption(PORT)) {
      return Integer.parseInt(cmd.getOptionValue(PORT));
    } else {
      return DEFAULT_PORT;
    }
  }

  public static String getConfigDir(CommandLine cmd) {
    if (cmd.hasOption(CONFIG)) {
      return cmd.getOptionValue(CONFIG);
    } else {
      return DEFAULT_CONFIG_LOCATION;
    }
  }

  public static Set<String> getAddresses(CommandLine cmd) {
    Set<String> addresses = new HashSet<String>();
    if (cmd.hasOption(IP)) {
      String[] values = cmd.getOptionValues(IP);
      addresses.addAll(Arrays.asList(values));
    } else {
      addresses.add(DEFAULT_IP);
    }
    return addresses;
  }

  public static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();
    {
      Option option = new Option(SHORT_IP_OPTION, IP, true,
          "Listening ip addresses comma delimited (default " + DEFAULT_IP + ")");
      option.setValueSeparator(',');
      options.addOption(option);
    }
    {
      Option option = new Option(SHORT_CONFIG_OPTION, CONFIG, true,
          "Configuration directory (default " + DEFAULT_CONFIG_LOCATION + ")");
      options.addOption(option);
    }
    {
      Option option = new Option(SHORT_PORT_OPTION, PORT, true, "Listening port (default " + DEFAULT_PORT + ")");
      options.addOption(option);
    }
    CommandLineParser parser = new DefaultParser();
    return parser.parse(options, args);
  }

}
