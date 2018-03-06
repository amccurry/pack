package pack.distributed.storage;

import pack.iscsi.storage.StorageTargetManager;
import pack.iscsi.storage.StorageTargetManagerFactory;

public class PackStorageTargetManagerFactory extends StorageTargetManagerFactory {

  @Override
  public StorageTargetManager create() throws Exception {
    return new PackStorageTargetManager();
  }

}