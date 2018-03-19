package iscsi.cli;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.google.common.collect.ImmutableList;

import iscsi.cli.target.TargetCommand;
import iscsi.cli.volume.VolumeCommand;

public class PackCommandGroup extends Command {

  private static final String J = "j";
  private static final String JSON = "json";
  private static final ThreadLocal<Boolean> _json = new ThreadLocal<>();

  public PackCommandGroup() {
    super("pack");
  }

  @Override
  public Options getOptions() {
    Options options = new Options();
    options.addOption(J, JSON, false, "Json Output");
    return options;
  }

  @Override
  public List<Command> getCommands() throws Exception {
    return ImmutableList.of(new VolumeCommand(), new TargetCommand());
  }

  @Override
  protected void handleCommand(CommandLine commandLine) {
    if (commandLine.hasOption(JSON)) {
      _json.set(true);
    }
  }

  public static boolean outputJson() {
    Boolean b = _json.get();
    if (b == null || !b) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public String getDescription() {
    return "Pack command is used to manage and get information about pack volumes and server.";
  }

}
