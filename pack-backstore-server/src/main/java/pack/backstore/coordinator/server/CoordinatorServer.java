package pack.backstore.coordinator.server;

import java.io.Closeable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.backstore.thrift.common.BackstoreServiceExceptionHelper;
import pack.backstore.thrift.common.BaseServer;
import pack.backstore.thrift.generated.BackstoreCoordinatorService;
import pack.backstore.thrift.generated.BackstoreServiceException;
import pack.backstore.thrift.generated.FileLockInfoRequest;
import pack.backstore.thrift.generated.FileLockInfoResponse;
import pack.backstore.thrift.generated.RegisterFileRequest;
import pack.backstore.thrift.generated.RegisterFileResponse;
import pack.backstore.thrift.generated.ReleaseFileRequest;
import pack.util.PackLock;

public class CoordinatorServer extends BaseServer
    implements BackstoreCoordinatorService.Iface, Closeable, BackstoreServiceExceptionHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorServer.class);

  private final Map<String, String> _lockMap = new ConcurrentHashMap<>();
  private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock(true);

  public CoordinatorServer(CoordinatorServerConfig config) throws TTransportException {
    super(LOGGER, config);
  }

  @Override
  protected TProcessor createTProcessor() {
    return new BackstoreCoordinatorService.Processor<>(this);
  }

  @Override
  public FileLockInfoResponse fileLock(FileLockInfoRequest request) throws BackstoreServiceException, TException {
    try (PackLock c = PackLock.create(getReadLock())) {
      String filename = request.getFilename();
      if (filename == null) {
        throw filenameMissing();
      }
      String lockId = _lockMap.get(filename);
      if (lockId == null) {
        throw lockMissingForFilename(filename);
      }
      return new FileLockInfoResponse(lockId);
    } catch (Throwable t) {
      throw newException(t);
    }
  }

  @Override
  public RegisterFileResponse registerFileLock(RegisterFileRequest request)
      throws BackstoreServiceException, TException {
    try (PackLock c = PackLock.create(getWriteLock())) {
      String filename = request.getFilename();
      if (filename == null) {
        throw filenameMissing();
      }
      String lockId = _lockMap.get(filename);
      if (lockId != null) {
        throw lockAlreadyRegisteredForFilename(filename);
      }
      lockId = UUID.randomUUID()
                   .toString();
      _lockMap.put(filename, lockId);
      return new RegisterFileResponse(lockId);
    } catch (Throwable t) {
      throw newException(t);
    }
  }

  @Override
  public void releaseFileLock(ReleaseFileRequest request) throws BackstoreServiceException, TException {
    try (PackLock c = PackLock.create(getWriteLock())) {
      String filename = request.getFilename();
      if (filename == null) {
        throw filenameMissing();
      }
      String providedLockId = request.getLockId();
      if (providedLockId == null) {
        throw lockIdMissing();
      }
      String lockId = _lockMap.get(filename);
      if (lockId == null) {
        throw lockMissingForFilename(filename);
      }
      if (!providedLockId.equals(lockId)) {
        throw lockInvalidForFilename(filename, lockId, providedLockId);
      }
    } catch (Throwable t) {
      throw newException(t);
    }
  }

  @Override
  public void noop() throws BackstoreServiceException, TException {

  }

  private Lock getWriteLock() {
    return _lock.writeLock();
  }

  private Lock getReadLock() {
    return _lock.readLock();
  }

}
