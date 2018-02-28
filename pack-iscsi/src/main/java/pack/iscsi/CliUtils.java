package pack.iscsi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CliUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(CliUtils.class);

  public static Configuration getConfig(CommandLine cmd, Options options) throws IOException {
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
        return null;
      }
      for (File f : dir.listFiles((FilenameFilter) (dir1, name) -> name.endsWith(".xml"))) {
        if (f.isFile()) {
          configuration.addResource(new FileInputStream(f));
        }
      }
    }
    return configuration;
  }

  public static Options createHdfsOptions() {
    Options options = new Options();
    options.addOption("p", "path", true, "Hdfs path.");
    options.addOption("r", "remote", true, "Hdfs ugi remote user.");
    options.addOption("u", "current", false, "Hdfs ugi use current user.");
    options.addOption("k", "keytab", true, "Hdfs keytab.");
    options.addOption("P", "principal", true, "Hdfs principal.");
    options.addOption("c", "conf", true, "Hdfs configuration location.");
    return options;
  }

  public static void printUsageAndExit(Options options) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("", options);
    System.exit(1);
  }

  public static UserGroupInformation getUGI(CommandLine cmd, Options options, Configuration configuration)
      throws IOException {
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
      CliUtils.printUsageAndExit(options);
      return null;
    }

    String principal = null;
    if (cmd.hasOption('P')) {
      principal = cmd.getOptionValue('P');
      LOGGER.info("principal {}", principal);
    }

    String keytab = null;
    if (cmd.hasOption('k')) {
      keytab = cmd.getOptionValue('k');
      LOGGER.info("keytab {}", keytab);
    }

    if (principal != null || keytab != null) {
      if (!(principal != null && keytab != null)) {
        System.err.println("both principal and keytab need to be set for kerberos");
        CliUtils.printUsageAndExit(options);
        return null;
      } else if (current != null) {
        System.err.println("current can not be used with kerberos");
        CliUtils.printUsageAndExit(options);
        return null;
      } else if (remote != null) {
        System.err.println("remote can not be used with kerberos");
        CliUtils.printUsageAndExit(options);
      }
    }

    UserGroupInformation.setConfiguration(configuration);
    UserGroupInformation ugi;
    if (principal != null) {
      ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
      ugi.reloginFromKeytab();
    } else if (remote != null) {
      ugi = UserGroupInformation.createRemoteUser(remote);
    } else if (current != null) {
      ugi = UserGroupInformation.getCurrentUser();
    } else {
      ugi = UserGroupInformation.getLoginUser();
    }
    return ugi;
  }

  public static Path getRootPath(CommandLine cmd, Options options) {
    if (cmd.hasOption('p')) {
      String path = cmd.getOptionValue('p');
      LOGGER.info("path {}", path);
      return new Path(path);
    } else {
      System.err.println("path missing");
      CliUtils.printUsageAndExit(options);
      return null;
    }
  }

  public static String getParamOrFail(CommandLine cmd, Options options, String name) {
    if (cmd.hasOption(name)) {
      String s = cmd.getOptionValue(name);
      LOGGER.info("{} {}", name, s);
      return s;
    } else {
      System.err.printf("%s missing%f", name);
      CliUtils.printUsageAndExit(options);
      return null;
    }
  }
}
