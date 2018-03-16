package pack.iscsi.storage;

public abstract class StorageTargetManagerFactory {

  public abstract StorageTargetManager create(StorageModuleDelegateFactory factory) throws Exception;

}
