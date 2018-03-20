package iscsi.cli.target;

import java.util.List;

import org.apache.commons.cli.Options;

import com.google.common.collect.ImmutableList;

import iscsi.cli.Command;

public class TargetCommand extends Command {

  public TargetCommand() {
    super("target");
  }

  @Override
  public Options getOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    return options;
  }

  @Override
  public List<Command> getCommands() throws Exception {
    return ImmutableList.of(new TargetListCommand());
  }

  @Override
  public String getDescription() {
    return "Target command is used get information about the iscsi target servers";
  }

}
