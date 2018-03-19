package pack.file.storage;

import pack.iscsi.storage.StorageTargetManager;
import pack.iscsi.storage.StorageTargetManagerFactory;

public class FileStorageTargetManagerFactory extends StorageTargetManagerFactory {

  @Override
  public StorageTargetManager create() throws Exception {
    return new FileStorageTargetManager();
  }

}
