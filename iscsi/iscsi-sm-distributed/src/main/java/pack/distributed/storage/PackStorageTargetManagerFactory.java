package pack.distributed.storage;

import pack.iscsi.storage.StorageModuleDelegateFactory;
import pack.iscsi.storage.StorageTargetManager;
import pack.iscsi.storage.StorageTargetManagerFactory;

public class PackStorageTargetManagerFactory extends StorageTargetManagerFactory {

  @Override
  public StorageTargetManager create(StorageModuleDelegateFactory factory) throws Exception {
    return new PackStorageTargetManager(factory);
  }

}
