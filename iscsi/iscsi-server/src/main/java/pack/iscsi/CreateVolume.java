package pack.iscsi;

import java.security.PrivilegedExceptionAction;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.PackMetaData;
import pack.iscsi.storage.utils.PackUtils;

public class CreateVolume {

  public static void main(String[] args) throws Exception {
    String name = args[0];
    long length = Long.parseLong(args[1]);
    Configuration configuration = PackConfig.getConfiguration();
    UserGroupInformation ugi = PackConfig.getUgi();
    Path hdfsTarget = PackConfig.getHdfsTarget();
    Path volume = new Path(hdfsTarget, name);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {

      FileSystem fileSystem = volume.getFileSystem(configuration);
      String newTopicId = PackUtils.getTopic(name, UUID.randomUUID()
                                                       .toString());
      fileSystem.delete(volume, true);
      fileSystem.mkdirs(volume);
      PackMetaData.builder()
                  .length(length)
                  .blockSize(4096)
                  .topicId(newTopicId)
                  .serialId(PackUtils.generateSerialId()
                                     .toString())
                  .build()
                  .write(configuration, volume);
      return null;
    });
  }

}
