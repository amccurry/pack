package pack.distributed.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
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

import pack.iscsi.storage.BaseStorageTargetManager;
import pack.iscsi.storage.utils.PackUtils;

public class HdfsStorageTargetManager extends BaseStorageTargetManager {

  private static final String HDFS_CONF_PATH = "HDFS_CONF_PATH";
  private static final String XML = ".xml";
  private static final String HDFS_KERBEROS_KEYTAB = "HDFS_KERBEROS_KEYTAB";
  private static final String HDFS_KERBEROS_PRINCIPAL = "HDFS_KERBEROS_PRINCIPAL";
  private static final String HDFS_UGI_REMOTE_USER = "HDFS_UGI_REMOTE_USER";
  private static final String HDFS_UGI_CURRENT_USER = "HDFS_UGI_CURRENT_USER";
  private static final String HDFS_TARGET_PATH = "HDFS_TARGET_PATH";
  private final UserGroupInformation _ugi;
  private final Path _rootPath;
  private final Configuration _conf;

  public HdfsStorageTargetManager() throws IOException {
    String rootPath = PackUtils.getEnvFailIfMissing(HDFS_TARGET_PATH);
    _rootPath = new Path(rootPath);
    _ugi = getUgi();
    _conf = getConfiguration();
  }

  private Configuration getConfiguration() throws FileNotFoundException {
    String configPath = PackUtils.getEnv(HDFS_CONF_PATH);
    Configuration configuration = new Configuration();
    File file = new File(configPath);
    if (file.isDirectory()) {
      File[] listFiles = file.listFiles((FilenameFilter) (dir, name) -> name.endsWith(XML));
      for (File f : listFiles) {
        configuration.addResource(new FileInputStream(f));
      }
    }
    return configuration;
  }

  private UserGroupInformation getUgi() throws IOException {
    if (PackUtils.isEnvSet(HDFS_UGI_CURRENT_USER)) {
      return UserGroupInformation.getCurrentUser();
    }
    String remoteUser = PackUtils.getEnv(HDFS_UGI_REMOTE_USER);
    if (remoteUser != null) {
      return UserGroupInformation.createRemoteUser(remoteUser);
    }
    String user = PackUtils.getEnv(HDFS_KERBEROS_PRINCIPAL);
    if (user != null) {
      String path = PackUtils.getEnvFailIfMissing(HDFS_KERBEROS_KEYTAB);
      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, path);
    }
    return UserGroupInformation.getLoginUser();
  }

  @Override
  protected String getType() {
    return "hdfs";
  }

  @Override
  protected IStorageModule createNewStorageModule(String name) throws IOException {
    Path volumeDir = new Path(_rootPath, name);
    HdfsMetaData hdfsMetaData = getHdfsMetaData(volumeDir);
    return new HdfsStorageModule(name, hdfsMetaData, _conf, volumeDir);
  }

  private HdfsMetaData getHdfsMetaData(Path volumeDir) throws IOException {
    FileSystem fileSystem = volumeDir.getFileSystem(_conf);
    if (!fileSystem.exists(volumeDir)) {
      throw new FileNotFoundException("Volume path " + volumeDir + " not found");
    }
    return HdfsMetaData.read(_conf, volumeDir);
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
