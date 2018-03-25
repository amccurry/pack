package pack.file.storage;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jscsi.target.Target;
import org.jscsi.target.storage.IStorageModule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.iscsi.storage.BaseStorageTargetManager;
import pack.iscsi.storage.utils.PackUtils;

public class FileStorageTargetManager extends BaseStorageTargetManager {

  private static final String DEFAULT_BLOCK_SIZE = "4096";
  private static final String LOCAL_BLOCK_SIZE = "LOCAL_BLOCK_SIZE";
  private static final String LOCAL_TARGET_PATH = "LOCAL_TARGET_PATH";
  private static final String FILE = "file";
  private final File _file;
  private final int _blockSize;

  public FileStorageTargetManager() {
    String filePath = PackUtils.getPropertyFailIfMissing(LOCAL_TARGET_PATH);
    _blockSize = Integer.parseInt(PackUtils.getProperty(LOCAL_BLOCK_SIZE, DEFAULT_BLOCK_SIZE));
    _file = new File(filePath);
    _file.mkdirs();
  }

  @Override
  public Target getTarget(String targetName) {
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    throw new RuntimeException("Unknown");
//    return super.getTarget(targetName);
  }

  @Override
  public String getType() {
    return FILE;
  }

  @Override
  public IStorageModule createNewStorageModule(String name) throws IOException {
    File file = new File(_file, name);
    long sizeInBytes = file.length();
    return new FileStorageModule(sizeInBytes, _blockSize, name, file);
  }

  @Override
  public List<String> getVolumeNames() {
    File[] listFiles = _file.listFiles((FileFilter) pathname -> pathname.isFile());
    Builder<String> builder = ImmutableList.builder();
    for (File file : listFiles) {
      builder.add(file.getName());
    }
    return builder.build();
  }

}
