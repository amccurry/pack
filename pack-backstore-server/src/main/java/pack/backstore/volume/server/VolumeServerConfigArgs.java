package pack.backstore.volume.server;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import pack.backstore.config.ServerConfigArgs;
import pack.backstore.volume.server.VolumeServerConfig.VolumeServerConfigBuilder;

public class VolumeServerConfigArgs {

  public static VolumeServerConfig create(String[] args) throws Exception {
    Options options = new Options();
    ServerConfigArgs.addServerOptions(options, VolumeServerConfig.builder()
                                                                 .build());
    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);
    VolumeServerConfigBuilder builder = VolumeServerConfig.builder();
    ServerConfigArgs.configureOptions(commandLine, builder);
    return builder.build();
  }

}
