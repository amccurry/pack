package iscsi.cli.volume;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;

import iscsi.cli.Command;
import iscsi.cli.PackCommandGroup;
import pack.distributed.storage.PackConfig;
import pack.distributed.storage.PackMetaData;

public class VolumeListCommand extends Command {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Path _hdfsTarget;
  private final Configuration _configuration;

  public VolumeListCommand() throws IOException {
    super("ls");
    _hdfsTarget = PackConfig.getHdfsTarget();
    _configuration = PackConfig.getConfiguration();
  }

  @Override
  protected void handleCommand(CommandLine commandLine) throws Exception {
    FileSystem fileSystem = _hdfsTarget.getFileSystem(_configuration);
    FileStatus[] listStatus = fileSystem.listStatus(_hdfsTarget);
    if (!PackCommandGroup.outputJson()) {
      System.out.printf("%-30s %-22s %-11s %-22s%n", "Volume Name", "Size", "Block Size", "Actual Size");
    } else {
      System.out.print("[");
    }
    boolean comma = false;
    for (FileStatus fileStatus : listStatus) {
      Path volume = fileStatus.getPath();
      PackMetaData metaData = PackMetaData.read(_configuration, volume);
      ContentSummary contentSummary = fileSystem.getContentSummary(volume);
      if (!PackCommandGroup.outputJson()) {
        System.out.printf("%-30s %,-22d %-11d %,-22d%n", volume.getName(), metaData.getLength(),
            metaData.getBlockSize(), contentSummary.getLength());
      } else {
        if (comma) {
          System.out.print(',');
        }
        VolumeInfo info = VolumeInfo.builder()
                                    .actualSize(contentSummary.getLength())
                                    .blockSize(metaData.getBlockSize())
                                    .name(volume.getName())
                                    .size(metaData.getLength())
                                    .build();
        System.out.print(MAPPER.writeValueAsString(info));
        comma = true;
      }
    }
    if (PackCommandGroup.outputJson()) {
      System.out.println("]");
    }
  }

  @Override
  public String getDescription() {
    return "List volumes";
  }

}
