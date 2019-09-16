package pack.iscsi.file.external.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.file.external.storage.LocalLogFileReaderWriter.LocalLogReader;
import pack.iscsi.file.external.storage.LocalLogFileReaderWriter.LocalLogWriter;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.RandomAccessIO;

public class LocalWriteAheadLogger implements WriteAheadLogger {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalWriteAheadLogger.class);

  private final AtomicReference<LocalLogWriter> _writer = new AtomicReference<>();
  private final File _blockLogDir;
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private final int _maxWalSize = 128 * 1024 * 1024;
  private final long _volumeId;
  private final long _blockId;

  public LocalWriteAheadLogger(File blockLogDir, long volumeId, long blockId) {
    _blockLogDir = blockLogDir;
    _volumeId = volumeId;
    _blockId = blockId;
    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeLock = reentrantReadWriteLock.writeLock();
    _readLock = reentrantReadWriteLock.readLock();
  }

  @Override
  public void append(long generation, long position, byte[] bytes, int offset, int len) throws IOException {
    _readLock.lock();
    try {
      LocalLogWriter writer = getLocalLogWriter(generation);
      writer.append(generation, position, bytes, offset, len);
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public void release(long generation) throws IOException {
    _writeLock.lock();
    LOGGER.info("release volumeId {} blockId {} generation {}", _volumeId, _blockId, generation);
    try {
      List<LocalLogReader> readers = getLocalLogReaders();
      try {
        for (LocalLogReader reader : readers) {
          if (reader.getMaxGeneration() <= generation) {
            File file = reader.getFile();
            LOGGER.info("Removing old log file {}", file);
            file.delete();
          }
        }
      } finally {
        IOUtils.close(LOGGER, readers);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public long recover(RandomAccessIO randomAccessIO, long onDiskGeneration) throws IOException {
    _writeLock.lock();
    LOGGER.info("recover volumeId {} blockId {}", _volumeId, _blockId);
    try {
      IOUtils.close(LOGGER, _writer.get());
      _writer.set(null);
      long currentGeneration = onDiskGeneration;
      List<LocalLogReader> readers = getLocalLogReaders();
      try {
        for (LocalLogReader reader : readers) {
          currentGeneration = recover(randomAccessIO, reader, currentGeneration);
        }
        return currentGeneration;
      } finally {
        IOUtils.close(LOGGER, readers);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("close volumeId {} blockId {}", _volumeId, _blockId);
    IOUtils.close(LOGGER, _writer.get());
  }

  private long recover(RandomAccessIO randomAccessIO, LocalLogReader reader, long currentGeneration)
      throws IOException {
    if (reader.getMaxGeneration() > currentGeneration) {
      reader.reset();
      while (reader.next()) {
        long generation = reader.getGeneration();
        if (generation > currentGeneration) {
          long position = reader.getPosition();
          randomAccessIO.writeFully(position, reader.getBytes(), 0, reader.getLength());
          currentGeneration = generation;
        }
      }
    }
    return currentGeneration;
  }

  private List<LocalLogReader> getLocalLogReaders() throws IOException {
    File[] files = _blockLogDir.listFiles();
    List<LocalLogReader> result = new ArrayList<>();
    for (File file : files) {
      result.add(new LocalLogReader(file));
    }
    Collections.sort(result);
    return result;
  }

  private synchronized LocalLogWriter getLocalLogWriter(long generation) throws IOException {
    LocalLogWriter writer = _writer.get();
    if (isValid(writer, generation)) {
      return writer;
    }
    IOUtils.close(LOGGER, writer);
    writer = new LocalLogWriter(_blockLogDir);
    _writer.set(writer);
    return writer;
  }

  private boolean isValid(LocalLogWriter writer, long generation) {
    if (writer == null) {
      return false;
    }
    if (writer.getSize() >= _maxWalSize) {
      return false;
    }
    return writer.getLastGeneration() + 1 == generation;
  }

}
