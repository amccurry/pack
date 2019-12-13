package pack.backstore.file.server;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import pack.backstore.file.server.FileServerConfig.FileServerConfigBuilder;

public class FileServerConfigArgs {

  private static final String STORE_DIR_LONG = "storedir";
  private static final String STORE_DIR_SHORT = "d";

  public static FileServerConfig create(String[] args) throws ParseException, IOException {
    Options options = new Options();
    {
      Option option = new Option(STORE_DIR_SHORT, STORE_DIR_LONG, true, "Store directory");
      option.setRequired(true);
      options.addOption(option);
    }

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    FileServerConfigBuilder builder = FileServerConfig.builder();
    if (commandLine.hasOption(STORE_DIR_SHORT)) {
      String dir = commandLine.getOptionValue(STORE_DIR_SHORT);
      File storeDir = new File(dir);
      storeDir.mkdir();
      if (!storeDir.exists()) {
        throw new IOException("Store dir does not exist " + storeDir);
      }
      builder.storeDir(storeDir);
    }
    return builder.build();
  }

}
