package iscsi.cli.volume;

import java.util.List;

import org.apache.commons.cli.Options;

import com.google.common.collect.ImmutableList;

import iscsi.cli.Command;

public class VolumeCommand extends Command {

  private static final String VOLUME = "volume";

  public VolumeCommand() {
    super(VOLUME);
  }

  @Override
  public Options getOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    return options;
  }

  @Override
  public List<Command> getCommands() throws Exception {
    return ImmutableList.of(new VolumeListCommand(), new VolumeCreateCommand());
  }

  @Override
  public String getDescription() {
    return "Volume command is used to manage pack volumes (list, create,  etc)";
  }

}
