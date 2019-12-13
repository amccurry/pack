package pack.backstore.file.server;

import pack.backstore.thrift.generated.BackstoreServiceException;

public interface LockIdManager {

  public static LockIdManager NO_LOCKS = new LockIdManager() {
  };

  default void validateLockId(String filename, String lockId) throws BackstoreServiceException {
    throw new RuntimeException("Not implemented");
  }

}
