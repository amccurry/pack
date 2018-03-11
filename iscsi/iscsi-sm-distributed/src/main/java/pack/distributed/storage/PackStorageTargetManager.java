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
import pack.distributed.storage.monitor.PackWriteBlockMonitorFactory;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.trace.TraceStorageModule;
import pack.iscsi.storage.BaseStorageTargetManager;
import pack.iscsi.storage.utils.PackUtils;

public class PackStorageTargetManager extends BaseStorageTargetManager {

  private final UserGroupInformation _ugi;
  private final Path _rootPath;
  private final Configuration _conf;
  private final PackKafkaClientFactory _packKafkaClientFactory;
  private final File _cacheDir;
  private final String _serialId;
  private final PackWriteBlockMonitorFactory _packWriteBlockMonitorFactory;

  public PackStorageTargetManager() throws IOException {
    _cacheDir = PackConfig.getWalCachePath();
    _ugi = PackConfig.getUgi();
    _conf = PackConfig.getConfiguration();
    _rootPath = PackConfig.getHdfsTarget();
    _serialId = PackConfig.getPackSerialId();

    String kafkaZkConnection = PackConfig.getKafkaZkConnection();
    _packKafkaClientFactory = new PackKafkaClientFactory(kafkaZkConnection);
    _packWriteBlockMonitorFactory = new PackWriteBlockMonitorFactory();
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
        PackUtils.rmr(cacheDir);
        cacheDir.mkdirs();
        WriteBlockMonitor monitor = _packWriteBlockMonitorFactory.create(name);
        return TraceStorageModule.traceIfEnabled(new PackStorageModule(name, _serialId, metaData, _conf, volumeDir,
            _packKafkaClientFactory, _ugi, cacheDir, monitor));
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
