package iscsi.cli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public abstract class Command implements Comparable<Command> {

  private String _name;

  protected Command(String name) {
    _name = name;
  }

  public Options getOptions() throws Exception {
    return new Options();
  }

  public void printHelp() throws Exception {
    List<Command> list = getCommands();
    List<Command> commands = new ArrayList<>();
    if (list != null) {
      commands.addAll(list);
    }
    Collections.sort(commands);
    if (!commands.isEmpty()) {
      System.err.println("Missing command:");
      for (Command command : commands) {
        System.err.printf("%-20s %s%n", command.getName(), command.getDescription());
      }
    }
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(getName() + " [OPTIONS] " + (commands.isEmpty() ? "" : commands), getOptions());
  }

  public List<Command> getCommands() throws Exception {
    return null;
  }

  public String getName() {
    return _name;
  }

  public final Command getCommand(String name) throws Exception {
    List<Command> commands = getCommands();
    if (commands == null) {
      return null;
    }
    for (Command commandGroup : commands) {
      if (commandGroup.getName()
                      .equals(name)) {
        return commandGroup;
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public final void process(List<String> args) throws Exception {
    // if (args.size() == 0) {
    // printHelp();
    // return;
    // }
    PosixParser parser = new PosixParser();
    CommandLine commandLine = parser.parse(getOptions(), args.toArray(new String[] {}), true);
    handleCommand(commandLine);
    List<String> subArgsList = commandLine.getArgList();
    if (!subArgsList.isEmpty()) {
      String subCommand = subArgsList.get(0);
      Command cmd = getCommand(subCommand);
      if (cmd != null) {
        cmd.process(removeHead(subArgsList));
      } else {
        printHelp();
      }
    } else {
      List<Command> commands = getCommands();
      if (commands != null && !commands.isEmpty()) {
        printHelp();
      }
    }
  }

  protected void handleCommand(CommandLine commandLine) throws Exception {
    
  }

  private List<String> removeHead(List<String> subArgsList) {
    return subArgsList.subList(1, subArgsList.size());
  }

  public abstract String getDescription();

  @Override
  public int compareTo(Command o) {
    return getName().compareTo(o.getName());
  }

  @Override
  public String toString() {
    return _name;
  }

}
