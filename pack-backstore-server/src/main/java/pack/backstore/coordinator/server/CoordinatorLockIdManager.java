package pack.backstore.coordinator.server;

import pack.backstore.coordinator.client.CoordinatorServiceClient;
import pack.backstore.coordinator.client.CoordinatorServiceClientConfig;
import pack.backstore.file.server.LockIdManager;
import pack.backstore.thrift.common.BackstoreServiceExceptionHelper;
import pack.backstore.thrift.common.ClientFactory;
import pack.backstore.thrift.generated.BackstoreServiceException;
import pack.backstore.thrift.generated.FileLockInfoRequest;
import pack.backstore.thrift.generated.FileLockInfoResponse;

public class CoordinatorLockIdManager implements LockIdManager, BackstoreServiceExceptionHelper {

  private final CoordinatorServiceClientConfig _config;

  public CoordinatorLockIdManager(CoordinatorServiceClientConfig config) {
    _config = config;
  }

  @Override
  public void validateLockId(String filename, String lockId) throws BackstoreServiceException {
    try (CoordinatorServiceClient client = ClientFactory.create(_config)) {
      FileLockInfoResponse fileLockInfo = client.fileLock(new FileLockInfoRequest(filename));
      String goodLockId = fileLockInfo.getLockId();
      if (!lockId.equals(goodLockId)) {
        throw lockInvalidForFilename(filename, goodLockId, lockId);
      }
    } catch (Throwable t) {
      throw newException(t);
    }
  }

}
