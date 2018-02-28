package pack.iscsi;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.block.blockstore.hdfs.CreateVolumeRequest;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsMetaData;

public class CreateVolume {

  public static void main(String[] args) throws ParseException, IOException, InterruptedException {
    Options options = CliUtils.createHdfsOptions();
    options.addOption("v", "volume", true, "Pack volume name.");
    options.addOption("l", "length", true, "Length in bytes.");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    Configuration configuration = CliUtils.getConfig(cmd, options);
    UserGroupInformation ugi = CliUtils.getUGI(cmd, options, configuration);
    Path root = CliUtils.getRootPath(cmd, options);
    String lenStr = CliUtils.getParamOrFail(cmd, options, "length");
    String volume = CliUtils.getParamOrFail(cmd, options, "volume");

    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      long length = Long.parseLong(lenStr);
      HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                            .fileSystemBlockSize(512)
                                                            .length(length)
                                                            .build();
      FileSystem fileSystem = root.getFileSystem(configuration);
      Path volumePath = new Path(root, volume);
      CreateVolumeRequest request = CreateVolumeRequest.builder()
                                                       .metaData(metaData)
                                                       .volumeName(volume)
                                                       .volumePath(volumePath)
                                                       .build();
      HdfsBlockStoreAdmin.createVolume(fileSystem, request);
      return null;
    });

  }

}
