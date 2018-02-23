package pack.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.hdfs.CreateVolumeRequest;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsMetaData;

public class CreateVolume {

  private static final String PACK = "pack";
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateVolume.class);

  public static void main(String[] args) throws IOException, ParseException, InterruptedException {

    Options options = new Options();
    options.addOption("p", "path", true, "Hdfs path.");
    options.addOption("r", "remote", true, "Hdfs ugi remote user.");
    options.addOption("u", "current", false, "Hdfs ugi use current user.");
    options.addOption("c", "conf", true, "Hdfs configuration location.");
    options.addOption("v", "volume", true, "Volume name.");

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

    String volumeName;
    if (cmd.hasOption('v')) {
      volumeName = cmd.getOptionValue('v');
      LOGGER.info("volumeName {}", volumeName);
    } else {
      System.err.println("volumeName missing");
      printUsageAndExit(options);
      return;
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

    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      Path root = new Path(path);
      FileSystem fileSystem = root.getFileSystem(configuration);

      Path volumePath = new Path(root, volumeName);
      HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA;
      CreateVolumeRequest request = CreateVolumeRequest.builder()
                                                       .volumeName(volumeName)
                                                       .volumePath(volumePath)
                                                       .metaData(metaData)
                                                       .build();
      HdfsBlockStoreAdmin.createVolume(fileSystem, request);
      return null;
    });

  }

  private static void printUsageAndExit(Options options) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(PACK, options);
    System.exit(1);
  }
}
