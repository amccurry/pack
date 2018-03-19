package iscsi.cli;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.PackMetaData;

public class Main {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws ParseException, IOException, InterruptedException {
    CommandLineParser parser = new PosixParser();
    Options options = new Options();
    CommandLine commandLine = parser.parse(options, args);

    List<String> list = commandLine.getArgList();
    if (list.isEmpty()) {
      printUsage();
      return;
    }
    Configuration configuration = PackConfig.getConfiguration();
    UserGroupInformation ugi = PackConfig.getUgi();
    Path hdfsTarget = PackConfig.getHdfsTarget();
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        switch (list.get(0)) {
        case "list": {
          list(configuration, hdfsTarget);
          break;
        }

        case "add": {
          break;
        }

        case "remove": {
          break;
        }

        case "update": {
          break;
        }

        default: {
          printUsage();
          return null;
        }
        }
        return null;
      }
    });
  }

  public static void list(Configuration configuration, Path hdfsTarget) throws IOException {
    FileSystem fileSystem = hdfsTarget.getFileSystem(configuration);
    FileStatus[] listStatus = fileSystem.listStatus(hdfsTarget);
    for (FileStatus fileStatus : listStatus) {
      Path volume = fileStatus.getPath();
      PackMetaData metaData = PackMetaData.read(configuration, volume);
      ContentSummary contentSummary = fileSystem.getContentSummary(volume);
      System.out.printf("%s %d %d %d%n", volume.getName(), metaData.getLength(), metaData.getBlockSize(),
          contentSummary.getLength());
    }
  }

  private static void printUsage() {

  }

}
