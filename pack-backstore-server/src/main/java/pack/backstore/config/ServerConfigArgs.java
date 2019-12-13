package pack.backstore.config;

import java.lang.reflect.Method;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ServerConfigArgs {

  public static void addServerOptions(Options options, ServerConfig defaultConfig) {
    {
      Option option = new Option("H", "hostname", true, "Bind host (default " + defaultConfig.getHostname() + ")");
      options.addOption(option);
    }

    {
      Option option = new Option("P", "port", true, "Listening port (default " + defaultConfig.getPort() + ")");
      options.addOption(option);
    }

    {
      Option option = new Option("T", "rpctimeout", true,
          "Client timeout (default " + defaultConfig.getClientTimeout() + ")");
      options.addOption(option);
    }

    {
      Option option = new Option("m", "minthreads", true,
          "Client timeout (default " + defaultConfig.getMinThreads() + ")");
      options.addOption(option);
    }

    {
      Option option = new Option("M", "maxthreads", true,
          "Client timeout (default " + defaultConfig.getMaxThreads() + ")");
      options.addOption(option);
    }
  }

  public static void configureOptions(CommandLine commandLine, Object builder) throws Exception {
    setString(commandLine, builder, "hostname", "hostname");
    setInteger(commandLine, builder, "port", "port");
    setInteger(commandLine, builder, "rpctimeout", "clientTimeout");
    setInteger(commandLine, builder, "minthreads", "minThreads");
    setInteger(commandLine, builder, "maxthreads", "maxThreads");
  }

  private static void setString(CommandLine commandLine, Object builder, String optionName, String methodName)
      throws Exception {
    if (commandLine.hasOption(optionName)) {
      Method method = builder.getClass()
                             .getDeclaredMethod(methodName, String.class);
      method.invoke(builder, commandLine.getOptionValue(optionName));
    }
  }

  private static void setInteger(CommandLine commandLine, Object builder, String optionName, String methodName)
      throws Exception {
    if (commandLine.hasOption(optionName)) {
      Method method = builder.getClass()
                             .getDeclaredMethod(methodName, Integer.TYPE);
      method.invoke(builder, Integer.parseInt(commandLine.getOptionValue(optionName)));
    }
  }

}
