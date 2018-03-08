package pack.distributed.storage;

import java.security.PrivilegedExceptionAction;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.iscsi.storage.utils.PackUtils;

public class CreateVolume {

  public static void main(String[] args) throws Exception {
    String name = "test";
    Configuration configuration = PackConfig.getConfiguration();
    UserGroupInformation ugi = PackConfig.getUgi();
    Path volume = new Path("/tmp/testpack/" + name);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {

      FileSystem fileSystem = volume.getFileSystem(configuration);
      String newTopicId = PackUtils.getTopic(name, UUID.randomUUID()
                                                       .toString());
      fileSystem.delete(volume, true);
      fileSystem.mkdirs(volume);
      PackMetaData.builder()
                  .length(100_000_000_000L)
                  .blockSize(4096)
                  .topicId(newTopicId)
                  .build()
                  .write(configuration, volume);
      return null;
    });

  }

}
