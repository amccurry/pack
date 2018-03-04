package pack.file.storage;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;

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
    String filePath = PackUtils.getEnvFailIfMissing(LOCAL_TARGET_PATH);
    _blockSize = Integer.parseInt(PackUtils.getEnv(LOCAL_BLOCK_SIZE, DEFAULT_BLOCK_SIZE));
    _file = new File(filePath);
    _file.mkdirs();
  }

  @Override
  protected String getType() {
    return FILE;
  }

  @Override
  protected IStorageModule createNewStorageModule(String name) throws IOException {
    File file = new File(_file, name);
    long sizeInBytes = file.length();
    return new FileStorageModule(sizeInBytes, _blockSize, name, file);
  }

  @Override
  protected List<String> getVolumeNames() {
    File[] listFiles = _file.listFiles((FileFilter) pathname -> pathname.isFile());
    Builder<String> builder = ImmutableList.builder();
    for (File file : listFiles) {
      builder.add(file.getName());
    }
    return builder.build();
  }

}
