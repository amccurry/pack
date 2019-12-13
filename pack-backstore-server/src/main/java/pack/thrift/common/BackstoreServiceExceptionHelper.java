package pack.thrift.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import pack.backstore.thrift.generated.BackstoreError;
import pack.backstore.thrift.generated.BackstoreServiceException;

public interface BackstoreServiceExceptionHelper {

  public default BackstoreServiceException newException(Throwable t) {
    return newException(BackstoreError.UNKNOWN, t.getMessage(), t);
  }

  public default BackstoreServiceException newException(BackstoreError error, String message, Throwable t) {
    if (t instanceof BackstoreServiceException) {
      return (BackstoreServiceException) t;
    }
    StringWriter writer = new StringWriter();
    try (PrintWriter pw = new PrintWriter(writer)) {
      t.printStackTrace(pw);
    }
    return createException(error, message, writer.toString());
  }

  public default BackstoreServiceException createError(BackstoreError error, String message) {
    return createException(error, message, null);
  }

  public default BackstoreServiceException createException(BackstoreError errorType, String message,
      String stackTraceStr) {
    return new BackstoreServiceException(errorType, message, stackTraceStr);
  }

  public default BackstoreServiceException filenameMissing() {
    return createError(BackstoreError.FILENAME_MISSING, "Filename missing in request");
  }

  public default BackstoreServiceException lockIdMissing() {
    return createError(BackstoreError.LOCK_ID_MISSING, "LockId missing in request");
  }

  public default BackstoreServiceException lockMissingForFilename(String filename) {
    return createError(BackstoreError.LOCK_MISSING, "Lock missing for filename " + filename);
  }

  public default BackstoreServiceException lockAlreadyRegisteredForFilename(String filename) {
    return createError(BackstoreError.LOCK_ALREADY_REGISTERED, "Lock already registered for filename " + filename);
  }

  public default BackstoreServiceException lockInvalidForFilename(String filename, String goodLockId,
      String badLockId) {
    return createError(BackstoreError.LOCK_ALREADY_INVALID,
        "Lock invalidate for filename " + filename + " good lockid " + goodLockId + " bad lockid" + badLockId);
  }

  public default BackstoreServiceException fileDeleteFailed(String filename) {
    return createError(BackstoreError.FILE_DELETE_FAILED, "Could not delete file " + filename);
  }

  public default BackstoreServiceException fileNotFound(String filename) {
    return createError(BackstoreError.FILE_NOT_FOUND, "File not found " + filename);
  }

  public default BackstoreServiceException fileExists(String filename) {
    return createError(BackstoreError.FILE_EXISTS, "File exists " + filename);
  }

  public default BackstoreServiceException ioError(String filename, IOException e) {
    return newException(BackstoreError.IO_ERROR, e.getMessage(), e);
  }

}
