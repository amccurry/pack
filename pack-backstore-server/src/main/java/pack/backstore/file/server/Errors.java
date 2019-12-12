package pack.backstore.file.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import pack.backstore.file.thrift.generated.BackstoreError;
import pack.backstore.file.thrift.generated.BackstoreFileServiceException;

public class Errors {

  public static BackstoreFileServiceException newBackstoreFileServiceException(Throwable t) {
    return newBackstoreFileServiceException(BackstoreError.UNKNOWN, t.getMessage(), t);
  }

  public static BackstoreFileServiceException newBackstoreFileServiceException(BackstoreError backstoreError,
      String message, Throwable t) {
    if (t instanceof BackstoreFileServiceException) {
      return (BackstoreFileServiceException) t;
    }
    StringWriter writer = new StringWriter();
    try (PrintWriter pw = new PrintWriter(writer)) {
      t.printStackTrace(pw);
    }
    return new BackstoreFileServiceException(backstoreError, message, writer.toString());
  }

  public static BackstoreFileServiceException createError(BackstoreError error, String message) {
    return new BackstoreFileServiceException(error, message, null);
  }

  public static BackstoreFileServiceException fileDeleteFailed(String filename) {
    return createError(BackstoreError.FILE_DELETE_FAILED, "Could not delete file " + filename);
  }

  public static BackstoreFileServiceException fileNotFound(String filename) {
    return createError(BackstoreError.FILE_NOT_FOUND, "File not found " + filename);
  }

  public static BackstoreFileServiceException fileExists(String filename) {
    return createError(BackstoreError.FILE_EXISTS, "File exists " + filename);
  }

  public static BackstoreFileServiceException ioError(String filename, IOException e) {
    return newBackstoreFileServiceException(BackstoreError.IO_ERROR, e.getMessage(), e);
  }
}
