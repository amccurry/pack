package pack.iscsi.file.singlefile;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import pack.iscsi.file.singlefile.FileStorageModule.FileStorageModuleFactory;
import pack.iscsi.spi.StorageModuleFactory;
import pack.iscsi.spi.StorageModuleLoader;

public class FileStorageModuleLoader extends StorageModuleLoader {

  public FileStorageModuleLoader() {
    super("file");
  }

  @Override
  public StorageModuleFactory create(String name, Properties properties) throws IOException {
    String pathStr = properties.getProperty("path");
    if (pathStr == null) {
      throw new IOException("path missing for file." + name + " storage module");
    }
    return new FileStorageModuleFactory(name, new File(pathStr));
  }

}
