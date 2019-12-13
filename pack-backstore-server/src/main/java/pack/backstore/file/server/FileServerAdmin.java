package pack.backstore.file.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.backstore.thrift.generated.BackstoreFileService;
import pack.backstore.thrift.generated.BackstoreServiceException;
import pack.backstore.thrift.generated.CreateFileRequest;
import pack.backstore.thrift.generated.DestroyFileRequest;
import pack.backstore.thrift.generated.ExistsFileRequest;
import pack.backstore.thrift.generated.ExistsFileResponse;
import pack.backstore.thrift.generated.ListFilesRequest;
import pack.backstore.thrift.generated.ListFilesResponse;
import pack.thrift.common.BackstoreServiceExceptionHelper;
import pack.thrift.common.BaseServer;
import pack.util.PackLock;

public abstract class FileServerAdmin extends BaseServer
    implements BackstoreFileService.Iface, Closeable, BackstoreServiceExceptionHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileServerAdmin.class);

  protected final ReentrantReadWriteLock _serviceLock = new ReentrantReadWriteLock(true);
  protected final File _storeDir;

  public FileServerAdmin(FileServerConfig config) throws TTransportException {
    this(LOGGER, config);
  }

  public FileServerAdmin(Logger logger, FileServerConfig config) throws TTransportException {
    super(logger, config.getHostname(), config.getPort(), config.getClientTimeout(), config.getMinThreads(),
        config.getMaxThreads());
    _storeDir = config.getStoreDir();
    _storeDir.mkdirs();
  }

  @Override
  protected TProcessor createTProcessor() {
    return new BackstoreFileService.Processor<>(this);
  }

  @Override
  public void create(CreateFileRequest request) throws BackstoreServiceException, TException {
    try (PackLock c = PackLock.create(getServiceWriteLock())) {
      String filename = request.getFilename();
      File file = new File(_storeDir, filename);
      if (file.exists()) {
        throw fileExists(filename);
      } else {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
          raf.setLength(request.getLength());
        }
      }
    } catch (Throwable t) {
      throw newException(t);
    }
  }

  @Override
  public ListFilesResponse listFiles(ListFilesRequest request) throws BackstoreServiceException, TException {
    try (PackLock c = PackLock.create(getServiceWriteLock())) {
      File[] listFiles = _storeDir.listFiles((FileFilter) pathname -> pathname.isFile());
      List<String> filenames = new ArrayList<>();
      for (File file : listFiles) {
        filenames.add(file.getName());
      }
      return new ListFilesResponse(filenames);
    } catch (Throwable t) {
      throw newException(t);
    }
  }

  @Override
  public void destroy(DestroyFileRequest request) throws BackstoreServiceException, TException {
    try (PackLock c = PackLock.create(getServiceWriteLock())) {
      String filename = request.getFilename();
      File file = new File(_storeDir, filename);
      if (file.exists()) {
        if (file.delete()) {
          removeFile(filename);
          return;
        } else {
          throw fileDeleteFailed(filename);
        }
      } else {
        throw fileNotFound(filename);
      }
    } catch (Throwable t) {
      throw newException(t);
    }
  }

  @Override
  public ExistsFileResponse exists(ExistsFileRequest request) throws BackstoreServiceException, TException {
    try (PackLock c = PackLock.create(getServiceWriteLock())) {
      String filename = request.getFilename();
      File file = new File(_storeDir, filename);
      return new ExistsFileResponse(file.exists());
    } catch (Throwable t) {
      throw newException(t);
    }
  }

  @Override
  public void noop() throws BackstoreServiceException, TException {
    // noop
  }

  protected WriteLock getServiceWriteLock() {
    return _serviceLock.writeLock();
  }

  protected ReadLock getServiceReadLock() {
    return _serviceLock.readLock();
  }

  protected void removeFile(String filename) {

  }
}
