package iscsi.cli.volume;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import iscsi.cli.Command;
import pack.distributed.storage.PackConfig;
import pack.distributed.storage.PackMetaData;
import pack.iscsi.storage.utils.PackUtils;

public class VolumeCreateCommand extends Command {

  private static final String J = "j";
  private static final String JSON = "json";
  private static final String B = "b";
  private static final String L = "l";
  private static final String V = "v";
  private static final String BLOCKSIZE = "blocksize";
  private static final String LENGTH = "length";
  private static final String VOLUME = "volume";

  private final Path _hdfsTarget;
  private final Configuration _configuration;

  public VolumeCreateCommand() throws IOException {
    super("create");
    _hdfsTarget = PackConfig.getHdfsTarget();
    _configuration = PackConfig.getConfiguration();
  }

  @Override
  public Options getOptions() {
    Options options = new Options();
    options.addOption(V, VOLUME, true, "Volume name");
    options.addOption(L, LENGTH, true, "Length (100,000,000,000)");
    options.addOption(B, BLOCKSIZE, true, "Block size (4096)");
    options.addOption(J, JSON, false, "Json Output");
    return options;
  }

  @Override
  protected void handleCommand(CommandLine commandLine) throws Exception {
    String name;
    if (commandLine.hasOption(VOLUME)) {
      name = commandLine.getOptionValue(VOLUME);
    } else {
      System.err.println("Missing --volume option.");
      System.exit(1);
      return;
    }

    int blockSize;
    if (commandLine.hasOption(BLOCKSIZE)) {
      blockSize = Integer.parseInt(commandLine.getOptionValue(BLOCKSIZE));
    } else {
      blockSize = PackMetaData.DEFAULT_BLOCK_SIZE;
    }

    long length;
    if (commandLine.hasOption(LENGTH)) {
      length = Long.parseLong(commandLine.getOptionValue(LENGTH));
    } else {
      length = PackMetaData.DEFAULT_LENGTH_BYTES;
    }

    String newTopicId = PackUtils.getTopic(name, UUID.randomUUID()
                                                     .toString());
    String serialId = PackUtils.generateSerialId()
                               .toString();
    PackMetaData metaData = PackMetaData.builder()
                                        .blockSize(blockSize)
                                        .length(length)
                                        .serialId(serialId)
                                        .topicId(newTopicId)
                                        .build();
    metaData.write(_configuration, new Path(_hdfsTarget, name));
  }

  @Override
  public String getDescription() {
    return "Create volume";
  }

}
