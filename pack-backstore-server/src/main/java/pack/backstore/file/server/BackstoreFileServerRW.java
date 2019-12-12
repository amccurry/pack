package pack.backstore.file.server;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;

import pack.backstore.file.thrift.generated.BackstoreFileServiceException;
import pack.backstore.file.thrift.generated.ReadFileRequest;
import pack.backstore.file.thrift.generated.ReadFileRequestBatch;
import pack.backstore.file.thrift.generated.ReadFileResponse;
import pack.backstore.file.thrift.generated.ReadFileResponseBatch;
import pack.backstore.file.thrift.generated.WriteFileRequest;
import pack.backstore.file.thrift.generated.WriteFileRequestBatch;
import pack.util.ExecutorUtil;
import pack.util.IOUtils;
import pack.util.PackLock;

public class BackstoreFileServerRW extends BackstoreFileServerAdmin {

  private static final Logger LOGGER = LoggerFactory.getLogger(BackstoreFileServerRW.class);
  private static final String RW = "rw";

  private final LoadingCache<String, FileHandle> _cache;

  public BackstoreFileServerRW(BackstoreFileServerConfig config) throws TTransportException {
    super(config);
    RemovalListener<String, FileHandle> removalListener = (key, value, cause) -> IOUtils.close(LOGGER, value);
    CacheLoader<String, FileHandle> loader = key -> {
      File file = new File(_storeDir, key);
      if (!file.exists()) {
        return null;
      }
      return new FileHandle(file);
    };
    _cache = Caffeine.newBuilder()
                     .removalListener(removalListener)
                     .maximumSize(config.getMaxFileHandles())
                     .executor(ExecutorUtil.getCallerRunExecutor())
                     .build(loader);
  }

  @Override
  public ReadFileResponseBatch read(ReadFileRequestBatch request) throws BackstoreFileServiceException, TException {
    try (PackLock c = PackLock.create(getServiceReadLock())) {
      return handleRead(request);
    } catch (Throwable t) {
      throw Errors.newBackstoreFileServiceException(t);
    }
  }

  private ReadFileResponseBatch handleRead(ReadFileRequestBatch request) throws BackstoreFileServiceException {
    FileHandle fileHandle = getFileHandle(request.getFilename());
    try (PackLock c = PackLock.create(getFileReadLock(fileHandle))) {
      FileChannel channel = fileHandle.getChannel();
      List<ReadFileResponse> responses = new ArrayList<>();
      List<ReadFileRequest> requests = request.getReadRequests();
      for (ReadFileRequest readFileRequest : requests) {
        try {
          responses.add(read(readFileRequest, channel));
        } catch (IOException e) {
          throw Errors.ioError(request.getFilename(), e);
        }
      }
      return new ReadFileResponseBatch(responses);
    }
  }

  @Override
  public void write(WriteFileRequestBatch request) throws BackstoreFileServiceException, TException {
    try (PackLock c = PackLock.create(getServiceReadLock())) {
      handleWrite(request);
    } catch (Throwable t) {
      throw Errors.newBackstoreFileServiceException(t);
    }
  }

  @Override
  protected void removeFile(String filename) {
    _cache.invalidate(filename);
  }

  private void handleWrite(WriteFileRequestBatch request) throws BackstoreFileServiceException {
    FileHandle fileHandle = getFileHandle(request.getFilename());
    try (PackLock c = PackLock.create(getFileWriteLock(fileHandle))) {
      FileChannel channel = fileHandle.getChannel();
      List<WriteFileRequest> writeRequests = request.getWriteRequests();
      for (WriteFileRequest writeFileRequest : writeRequests) {
        try {
          write(writeFileRequest, channel);
        } catch (IOException e) {
          throw Errors.ioError(request.getFilename(), e);
        }
      }
    }
  }

  private void write(WriteFileRequest writeFileRequest, FileChannel channel) throws IOException {
    ByteBuffer buffer = writeFileRequest.bufferForData();
    long position = writeFileRequest.getPosition();
    while (buffer.hasRemaining()) {
      position += channel.write(buffer, position);
    }
  }

  private ReadFileResponse read(ReadFileRequest readFileRequest, FileChannel channel) throws IOException {
    long position = readFileRequest.getPosition();
    ByteBuffer buffer = ByteBuffer.allocate(readFileRequest.getLength());
    while (buffer.hasRemaining()) {
      position += channel.read(buffer, position);
    }
    buffer.flip();
    return new ReadFileResponse(buffer);
  }

  private FileHandle getFileHandle(String filename) throws BackstoreFileServiceException {
    FileHandle fileHandle = _cache.get(filename);
    if (fileHandle == null) {
      throw Errors.fileNotFound(filename);
    }
    return fileHandle;
  }

  private ReadLock getFileReadLock(FileHandle fileHandle) {
    return fileHandle.getReadLock();
  }

  private WriteLock getFileWriteLock(FileHandle fileHandle) {
    return fileHandle.getWriteLock();
  }

  static class FileHandle implements Closeable {

    final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock(true);
    final RandomAccessFile _raf;
    final FileChannel _channel;

    FileHandle(File file) throws IOException {
      _raf = new RandomAccessFile(file, RW);
      _channel = _raf.getChannel();
    }

    WriteLock getWriteLock() {
      return _lock.writeLock();
    }

    ReadLock getReadLock() {
      return _lock.readLock();
    }

    FileChannel getChannel() {
      return _channel;
    }

    @Override
    public void close() throws IOException {
      _channel.close();
      _raf.close();
    }
  }

}
