package pack.iscsi.storage;

import org.jscsi.target.storage.IStorageModule;

public interface StorageModuleDelegateFactory {

  IStorageModule delegate(String name, IStorageModule module);

}
