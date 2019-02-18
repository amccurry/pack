package pack.block.blockstore.cli;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.block.blockstore.compactor.PackCompactorServer;
import pack.block.server.BlockPackStorage;
import pack.block.server.BlockPackStorageInfo;
import pack.block.util.Utils;

public class PackCli {

  public enum Command {
    HELP("list commands"),

    LIST("list volumes"),

    COMPACT("run a compaction process for a given volume"),

    CONVERT("run a converter process to convert WAL files to block files"),

    VOLUME("view/edit/create/remove the volume"),

    SNAPSHOT("list/create/remove spanshot"),

    LOG("view log files for a volume");

    private final String _desc;

    private Command(String desc) {
      _desc = desc;
    }

    public String getDesc() {
      return _desc;
    }
  }

  public static void main(String[] args) throws Exception {
    Utils.setupLog4j();
    Utils.loadPackProperties();
    if (args.length == 0) {
      printCommnadHelp();
      System.exit(1);
    }
    String commandStr = args[0];
    Command command;
    try {
      command = Command.valueOf(commandStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      System.err.printf("Command %s not found%n%n", commandStr);
      printCommnadHelp();
      System.exit(1);
      return;
    }
    switch (command) {
    case HELP:
      help(getCommandArgs(args));
      break;
    case LIST:
      list(getCommandArgs(args));
      break;
    case COMPACT:
      compact(getCommandArgs(args));
      break;
    case CONVERT:
      convert(getCommandArgs(args));
      break;
    case VOLUME:
      volume(getCommandArgs(args));
      break;
    case SNAPSHOT:
      snapshot(getCommandArgs(args));
      break;
    case LOG:
      log(getCommandArgs(args));
      break;
    default:
      break;
    }
  }

  private static void log(String[] args) throws Exception {
    // Options options = new Options();
    // options.addOption(new Option("h", "help", false, "show help"));
    // CommandLineParser parser = new PosixParser();
    // CommandLine cmd = parser.parse(options, args);

  }

  private static void snapshot(String[] args) {

  }

  private static void volume(String[] args) {

  }

  private static void convert(String[] args) {

  }

  @SuppressWarnings("unchecked")
  private static void compact(String[] args) throws Exception {
    Options options = new Options();
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    List<String> volumes = cmd.getArgList();
    if (volumes.isEmpty()) {
      System.err.println("Specify volume name(s)");
      System.exit(1);
    }
    for (String volume : volumes) {
      System.err.printf("Running compaction for %s%n", volume);
      runCompaction(volume);
    }
  }

  private static void runCompaction(String volume) throws Exception {
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    Configuration configuration = new Configuration();
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      Path volumePath = new Path(new Path(Utils.getHdfsPath()), volume);
      PackCompactorServer.executeCompactionVolume(configuration, volumePath);
      return null;
    });
  }

  private static void list(String[] args) throws Exception {
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    Configuration configuration = new Configuration();
    List<String> list = ugi.doAs((PrivilegedExceptionAction<List<String>>) () -> {
      Path root = new Path(Utils.getHdfsPath());
      FileSystem fileSystem = root.getFileSystem(configuration);
      return BlockPackStorage.listHdfsVolumes(fileSystem, root);
    });
    for (String volumeName : list) {
      BlockPackStorageInfo info = ugi.doAs((PrivilegedExceptionAction<BlockPackStorageInfo>) () -> {
        Path root = new Path(Utils.getHdfsPath());
        FileSystem fileSystem = root.getFileSystem(configuration);
        return BlockPackStorage.getBlockPackStorageInfo(fileSystem, root, volumeName);
      });
      System.out.printf("%s%n", volumeName);
    }
  }

  private static void help(String[] args) throws ParseException {
    printCommnadHelp();
  }

  private static String[] getCommandArgs(String[] args) {
    String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 1, newArgs, 0, newArgs.length);
    return newArgs;
  }

  private static void printCommnadHelp() {
    Command[] values = Command.values();
    Arrays.sort(values, (o1, o2) -> o1.name()
                                      .compareTo(o2.name()));
    for (Command command : values) {
      String niceName = command.name()
                               .toLowerCase();
      System.err.printf("%-20s %s%n", niceName, command.getDesc());
    }
    System.err.println();
  }

}
