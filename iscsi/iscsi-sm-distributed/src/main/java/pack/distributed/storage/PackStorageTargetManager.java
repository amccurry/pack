package pack.distributed.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.jscsi.target.storage.IStorageModule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.distributed.storage.kafka.PackKafkaClientFactory;
import pack.iscsi.storage.BaseStorageTargetManager;

public class PackStorageTargetManager extends BaseStorageTargetManager {

  private final UserGroupInformation _ugi;
  private final Path _rootPath;
  private final Configuration _conf;
  private final PackKafkaClientFactory _packKafkaClientFactory;
  private final File _cacheDir;

  public PackStorageTargetManager() throws IOException {
    _cacheDir = PackConfig.getWalCachePath();
    _ugi = PackConfig.getUgi();
    _conf = PackConfig.getConfiguration();
    _rootPath = PackConfig.getHdfsTarget();

    String kafkaZkConnection = PackConfig.getKafkaZkConnection();
    _packKafkaClientFactory = new PackKafkaClientFactory(kafkaZkConnection);
  }

  @Override
  protected String getType() {
    return "hdfs";
  }

  @Override
  protected IStorageModule createNewStorageModule(String name) throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<IStorageModule>) () -> {
        Path volumeDir = new Path(_rootPath, name);
        PackMetaData metaData = getMetaData(volumeDir);
        File cacheDir = new File(_cacheDir, name);
        cacheDir.mkdirs();
        return new PackStorageModule(name, metaData, _conf, volumeDir, _packKafkaClientFactory, _ugi, cacheDir);
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private PackMetaData getMetaData(Path volumeDir) throws IOException {
    FileSystem fileSystem = volumeDir.getFileSystem(_conf);
    if (!fileSystem.exists(volumeDir)) {
      throw new FileNotFoundException("Volume path " + volumeDir + " not found");
    }
    return PackMetaData.read(_conf, volumeDir);
  }

  @Override
  protected List<String> getVolumeNames() {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<List<String>>) () -> {
        Builder<String> builder = ImmutableList.builder();
        FileSystem fileSystem = _rootPath.getFileSystem(_conf);
        if (!fileSystem.exists(_rootPath)) {
          return builder.build();
        }
        FileStatus[] listStatus = fileSystem.listStatus(_rootPath);
        for (FileStatus fileStatus : listStatus) {
          if (fileStatus.isDirectory()) {
            Path path = fileStatus.getPath();
            builder.add(path.getName());
          }
        }
        return builder.build();
      });
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
