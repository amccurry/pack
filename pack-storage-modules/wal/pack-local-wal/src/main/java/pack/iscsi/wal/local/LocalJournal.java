package pack.iscsi.wal.local;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
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

import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.wal.BlockJournalRange;
import pack.iscsi.spi.wal.BlockRecoveryWriter;
import pack.iscsi.wal.local.LocalJournalReader.LocalLogReaderConfig;
import pack.iscsi.wal.local.LocalJournalWriter.LocalLogWriterConfig;

public class LocalJournal implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalJournal.class);

  private final AtomicReference<LocalJournalWriter> _writer = new AtomicReference<>();
  private final File _blockLogDir;
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private final int _maxWalSize = 128 * 1024 * 1024;
  private final long _volumeId;
  private final long _blockId;

  public LocalJournal(File blockLogDir, long volumeId, long blockId) {
    _blockLogDir = blockLogDir;
    _volumeId = volumeId;
    _blockId = blockId;
    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeLock = reentrantReadWriteLock.writeLock();
    _readLock = reentrantReadWriteLock.readLock();
  }

  public void append(long generation, long position, byte[] bytes, int offset, int len) throws IOException {
    _readLock.lock();
    try {
      LocalJournalWriter writer = getLocalLogWriter(generation);
      writer.append(generation, position, bytes, offset, len);
    } finally {
      _readLock.unlock();
    }
  }

  public void release(long generation) throws IOException {
    _writeLock.lock();
    LOGGER.info("release volumeId {} blockId {} generation {}", _volumeId, _blockId, generation);
    try {
      List<LocalJournalReader> readers = getLocalLogReaders();
      try {
        for (LocalJournalReader reader : readers) {
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

  public List<BlockJournalRange> getJournalRanges(long onDiskGeneration, boolean closeExistingWriter)
      throws IOException {
    _writeLock.lock();
    LOGGER.info("getJournalRanges volumeId {} blockId {}", _volumeId, _blockId);
    try {
      if (closeExistingWriter) {
        IOUtils.close(LOGGER, _writer.get());
        _writer.set(null);
      }
      List<LocalJournalReader> readers = getLocalLogReaders();
      try {
        List<BlockJournalRange> result = new ArrayList<>();
        for (LocalJournalReader reader : readers) {
          long maxGeneration = reader.getMaxGeneration();
          if (onDiskGeneration < maxGeneration) {
            String uuid = reader.getUuid();
            result.add(BlockJournalRange.builder()
                                        .blockId(_blockId)
                                        .maxGeneration(maxGeneration)
                                        .minGeneration(reader.getMinGeneration())
                                        .uuid(uuid)
                                        .volumeId(_volumeId)
                                        .build());
          }
        }
        return result;
      } finally {
        IOUtils.close(LOGGER, readers);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  public long recover(String uuid, RandomAccessIO randomAccessIO, long onDiskGeneration) throws IOException {
    return recover(uuid, BlockRecoveryWriter.toBlockRecoveryWriter(randomAccessIO), onDiskGeneration);
  }

  public long recover(String uuid, BlockRecoveryWriter writer, long onDiskGeneration) throws IOException {
    _writeLock.lock();
    LOGGER.info("recover volumeId {} blockId {} on disk generation {}", _volumeId, _blockId, onDiskGeneration);
    try {
      try (LocalJournalReader reader = getLocalLogReader(uuid)) {
        return recover(reader, writer, onDiskGeneration);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    LocalJournalWriter journalWriter = _writer.get();
    if (journalWriter != null) {
      LOGGER.info("close volumeId {} blockId {} last generation {}", _volumeId, _blockId,
          journalWriter.getLastGeneration());
      IOUtils.close(LOGGER, journalWriter);
    }
  }

  private long recover(LocalJournalReader reader, BlockRecoveryWriter writer, long onDiskGeneration)
      throws IOException {
    if (reader.getMaxGeneration() > onDiskGeneration) {
      reader.reset();
      while (reader.next()) {
        long generation = reader.getGeneration();
        if (generation > onDiskGeneration) {
          long position = reader.getPosition();
          if (!writer.writeEntry(generation, position, reader.getBytes(), 0, reader.getLength())) {
            return onDiskGeneration;
          }
          onDiskGeneration = generation;
        }
      }
    }
    return onDiskGeneration;
  }

  private List<LocalJournalReader> getLocalLogReaders() throws IOException {
    File[] files = _blockLogDir.listFiles();
    List<LocalJournalReader> result = new ArrayList<>();
    for (File file : files) {
      if (!isWriting(file)) {
        result.add(new LocalJournalReader(LocalLogReaderConfig.builder()
                                                              .blockLogFile(file)
                                                              .build()));
      }
    }
    Collections.sort(result);
    return result;
  }

  private LocalJournalReader getLocalLogReader(String uuid) throws IOException {
    File file = new File(_blockLogDir, uuid);
    if (!file.exists()) {
      throw new FileNotFoundException(file.getAbsolutePath());
    }
    return new LocalJournalReader(LocalLogReaderConfig.builder()
                                                      .blockLogFile(file)
                                                      .build());
  }

  private boolean isWriting(File file) {
    LocalJournalWriter localLogWriter = _writer.get();
    if (localLogWriter == null) {
      return false;
    }
    return localLogWriter.getFile()
                         .equals(file);
  }

  private synchronized LocalJournalWriter getLocalLogWriter(long generation) throws IOException {
    LocalJournalWriter writer = _writer.get();
    if (isValid(writer, generation)) {
      return writer;
    }
    IOUtils.close(LOGGER, writer);
    writer = new LocalJournalWriter(LocalLogWriterConfig.builder()
                                                        .blockLogDir(_blockLogDir)
                                                        .build());
    _writer.set(writer);
    return writer;
  }

  private boolean isValid(LocalJournalWriter writer, long generation) {
    if (writer == null) {
      return false;
    }
    if (writer.getSize() >= _maxWalSize) {
      return false;
    }
    return writer.getLastGeneration() + 1 == generation;
  }

  // public long recover(RandomAccessIO randomAccessIO, long onDiskGeneration)
  // throws IOException {
  // _writeLock.lock();
  // LOGGER.info("recover volumeId {} blockId {}", _volumeId, _blockId);
  // try {
  // IOUtils.close(LOGGER, _writer.get());
  // _writer.set(null);
  // long currentGeneration = onDiskGeneration;
  // List<LocalJournalReader> readers = getLocalLogReaders();
  // try {
  // for (LocalJournalReader reader : readers) {
  // currentGeneration = recover(randomAccessIO, reader, currentGeneration);
  // }
  // return currentGeneration;
  // } finally {
  // IOUtils.close(LOGGER, readers);
  // }
  // } finally {
  // _writeLock.unlock();
  // }
  // }

}
