/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pack.block.blockstore.hdfs.kvs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.hdfs.file.ReadRequest;

public class HdfsKeyValueStore implements Store {

  private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

  public static final int DEFAULT_MAX_AMOUNT_ALLOWED_PER_FILE = 64 * 1024 * 1024;
  public static final long DEFAULT_MAX_OPEN_FOR_WRITING = TimeUnit.MINUTES.toMillis(1);

  private static final String UTF_8 = "UTF-8";
  private static final String HDFS_KEY_VALUE = "hdfs_key_value";

  private static final byte[] MAGIC;
  private static final int VERSION = 1;
  private static final long DAEMON_POLL_TIME = TimeUnit.SECONDS.toMillis(5);
  private static final int VERSION_LENGTH = 4;

  static {
    try {
      MAGIC = HDFS_KEY_VALUE.getBytes(UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  static enum OperationType {
    PUT, DELETE
  }

  static class Operation implements Writable {

    OperationType type;
    LongWritable key = new LongWritable();
    BytesWritable value = new BytesWritable();

    @Override
    public void write(DataOutput out) throws IOException {
      if (type == OperationType.DELETE) {
        out.write(0);
        key.write(out);
      } else if (type == OperationType.PUT) {
        out.write(1);
        key.write(out);
        value.write(out);
      } else {
        throw new RuntimeException("Not supported [" + type + "]");
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      byte b = in.readByte();
      switch (b) {
      case 0:
        type = OperationType.DELETE;
        key.readFields(in);
        return;
      case 1:
        type = OperationType.PUT;
        key.readFields(in);
        value.readFields(in);
        return;
      default:
        throw new RuntimeException("Not supported [" + b + "]");
      }
    }

  }

  static class Value {
    Value(BytesWritable value, Path path) {
      _value = value;
      _path = path;
    }

    BytesWritable _value;
    Path _path;
  }

  private final ConcurrentNavigableMap<Long, Value> _pointers = new ConcurrentSkipListMap<Long, Value>();
  private final Path _path;
  private final ReentrantReadWriteLock _readWriteLock;
  private final AtomicReference<SortedSet<FileStatus>> _fileStatus = new AtomicReference<SortedSet<FileStatus>>();
  private final FileSystem _fileSystem;
  private final AtomicLong _currentFileCounter = new AtomicLong();
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private final AtomicLong _size = new AtomicLong();
  private final long _maxAmountAllowedPerFile;
  private final TimerTask _idleLogTimerTask;
  private final TimerTask _oldFileCleanerTimerTask;
  private final AtomicLong _lastWrite = new AtomicLong();
  private final Timer _hdfsKeyValueTimer;
  private final long _maxTimeOpenForWriting;
  private final boolean _readOnly;
  private final Logger _logger;
  private final ExecutorService _service;
  private final AtomicLong _lastKey = new AtomicLong();
  private final AtomicLong _lastKeyGreaterThanCount = new AtomicLong();
  private final AtomicLong _lastKeyEqualToCount = new AtomicLong();

  private FSDataOutputStream _output;
  private Path _outputPath;
  private boolean _isClosed;

  public HdfsKeyValueStore(String name, boolean readOnly, Timer hdfsKeyValueTimer, Configuration configuration,
      Path path) throws IOException {
    this(name, readOnly, hdfsKeyValueTimer, configuration, path, DEFAULT_MAX_AMOUNT_ALLOWED_PER_FILE,
        DEFAULT_MAX_OPEN_FOR_WRITING);
  }

  public HdfsKeyValueStore(String name, boolean readOnly, Timer hdfsKeyValueTimer, Configuration configuration,
      Path path, long maxAmountAllowedPerFile) throws IOException {
    this(name, readOnly, hdfsKeyValueTimer, configuration, path, maxAmountAllowedPerFile, DEFAULT_MAX_OPEN_FOR_WRITING);
  }

  public HdfsKeyValueStore(String name, boolean readOnly, Timer hdfsKeyValueTimer, Configuration configuration,
      Path path, long maxAmountAllowedPerFile, long maxTimeOpenForWriting) throws IOException {
    _logger = LoggerFactory.getLogger("HDFS/KVS/" + name);
    _service = Executors.newSingleThreadExecutor();
    _readOnly = readOnly;
    _maxTimeOpenForWriting = maxTimeOpenForWriting;
    _maxAmountAllowedPerFile = maxAmountAllowedPerFile;
    _path = path;
    _fileSystem = _path.getFileSystem(configuration);
    _fileSystem.mkdirs(_path);
    _readWriteLock = new ReentrantReadWriteLock();
    _writeLock = _readWriteLock.writeLock();
    _readLock = _readWriteLock.readLock();
    _fileStatus.set(getSortedSet(_path));
    if (!_fileStatus.get()
                    .isEmpty()) {
      _currentFileCounter.set(Long.parseLong(_fileStatus.get()
                                                        .last()
                                                        .getPath()
                                                        .getName()));
    }
    removeAnyTruncatedFiles();
    loadIndexes();
    cleanupOldFiles();
    if (!_readOnly) {
      _idleLogTimerTask = getIdleLogTimer();
      _oldFileCleanerTimerTask = getOldFileCleanerTimer();
      _hdfsKeyValueTimer = hdfsKeyValueTimer;
      _hdfsKeyValueTimer.schedule(_idleLogTimerTask, DAEMON_POLL_TIME, DAEMON_POLL_TIME);
      _hdfsKeyValueTimer.schedule(_oldFileCleanerTimerTask, DAEMON_POLL_TIME, DAEMON_POLL_TIME);
    } else {
      _idleLogTimerTask = null;
      _oldFileCleanerTimerTask = null;
      _hdfsKeyValueTimer = null;
    }
  }

  private void removeAnyTruncatedFiles() throws IOException {
    for (FileStatus fileStatus : _fileStatus.get()) {
      Path path = fileStatus.getPath();
      FSDataInputStream inputStream = _fileSystem.open(path);
      long len = HdfsUtils.getFileLength(_fileSystem, path, inputStream);
      inputStream.close();
      if (len < MAGIC.length + VERSION_LENGTH) {
        // Remove invalid file
        _logger.warn("Removing file {} because length of {} is less than MAGIC plus version length of {}", path, len,
            MAGIC.length + VERSION_LENGTH);
        _fileSystem.delete(path, false);
      }
    }
  }

  private TimerTask getOldFileCleanerTimer() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          cleanupOldFiles();
        } catch (Throwable e) {
          _logger.error("Unknown error while trying to clean up old files.", e);
        }
      }
    };
  }

  private TimerTask getIdleLogTimer() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          closeLogFileIfIdle();
        } catch (Throwable e) {
          _logger.error("Unknown error while trying to close output file.", e);
        }
      }

    };
  }

  @Override
  public void sync(boolean sync) throws IOException {
    if (sync) {
      ensureOpen();
      _writeLock.lock();
      ensureOpenForWriting();
      try {
        syncInternal();
      } catch (RemoteException e) {
        throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
      } catch (LeaseExpiredException e) {
        throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
      } finally {
        _writeLock.unlock();
      }
    } else {
      _service.submit(() -> {
        try {
          syncInternal();
        } catch (Exception e) {
          _logger.error("Unknown error while syncing data.", e);
        }
      });
    }
  }

  @Override
  public void flush(boolean sync) throws IOException {
    if (sync) {
      ensureOpen();
      _writeLock.lock();
      ensureOpenForWriting();
      try {
        flushInternal();
      } catch (RemoteException e) {
        throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
      } catch (LeaseExpiredException e) {
        throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
      } finally {
        _writeLock.unlock();
      }
    } else {
      _service.submit(() -> {
        try {
          flushInternal();
        } catch (Exception e) {
          _logger.error("Unknown error while syncing data.", e);
        }
      });
    }
  }

  @Override
  public void put(long key, ByteBuffer value) throws IOException {
    ensureOpen();
    if (value == null) {
      delete(key);
      return;
    }
    _writeLock.lock();
    if (key > _lastKey.get()) {
      _lastKeyGreaterThanCount.incrementAndGet();
    } else if (key == _lastKey.get()) {
      _lastKeyEqualToCount.incrementAndGet();
    } else {
      _logger.info("In-order write reset. > {} = {}", _lastKeyGreaterThanCount.get(), _lastKeyEqualToCount.get());
      _lastKeyGreaterThanCount.set(0);
      _lastKeyEqualToCount.set(0);
    }
    _lastKey.set(key);
    ensureOpenForWriting();
    try {
      Operation op = getPutOperation(OperationType.PUT, key, value);
      Path path = write(op);
      BytesWritable bwValue = op.value;
      _size.addAndGet(bwValue.getCapacity());
      Value old = _pointers.put(key, new Value(bwValue, path));
      if (old != null) {
        _size.addAndGet(-old._value.getCapacity());
      }
    } catch (RemoteException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } catch (LeaseExpiredException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } finally {
      _writeLock.unlock();
    }
  }

  private void ensureOpenForWriting() throws IOException {
    if (_output == null) {
      openWriter();
    }
  }

  private Path write(Operation op) throws IOException {
    op.write(_output);
    Path p = _outputPath;
    if (_output.getPos() >= _maxAmountAllowedPerFile) {
      rollFile();
    }
    return p;
  }

  private void rollFile() throws IOException {
    _logger.info("Rolling file {}", _outputPath);
    _output.close();
    _output = null;
    openWriter();
  }

  public void cleanupOldFiles() throws IOException {
    _writeLock.lock();
    try {
      if (!isOpenForWriting()) {
        return;
      }
      SortedSet<FileStatus> fileStatusSet = getSortedSet(_path);
      if (fileStatusSet == null || fileStatusSet.size() < 1) {
        return;
      }
      Path newestGen = fileStatusSet.last()
                                    .getPath();
      if (!newestGen.equals(_outputPath)) {
        throw new IOException("No longer the owner of [" + _path + "]");
      }
      Set<Path> existingFiles = new HashSet<Path>();
      for (FileStatus fileStatus : fileStatusSet) {
        existingFiles.add(fileStatus.getPath());
      }
      Set<Entry<Long, Value>> entrySet = _pointers.entrySet();
      existingFiles.remove(_outputPath);
      for (Entry<Long, Value> e : entrySet) {
        Path p = e.getValue()._path;
        existingFiles.remove(p);
      }
      for (Path p : existingFiles) {
        _logger.info("Removing file no longer referenced {}", p);
        _fileSystem.delete(p, false);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private void closeLogFileIfIdle() throws IOException {
    _writeLock.lock();
    try {
      if (_output != null && _lastWrite.get() + _maxTimeOpenForWriting < System.currentTimeMillis()) {
        // Close writer
        _logger.info("Closing KV log due to inactivity {}.", _path);
        try {
          _output.close();
        } finally {
          _output = null;
        }
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private boolean isOpenForWriting() {
    return _output != null;
  }

  private Operation getPutOperation(OperationType put, long key, ByteBuffer value) {
    if (isAllZeros(value)) {
      value = EMPTY;
    }
    Operation operation = new Operation();
    operation.type = put;
    operation.key.set(key);
    int remaining = value.remaining();
    byte[] buffer = new byte[remaining];
    value.get(buffer);
    operation.value.set(buffer, 0, remaining);
    return operation;
  }

  private boolean isAllZeros(ByteBuffer value) {
    byte[] bs = value.array();
    int limit = value.limit();
    int position = value.position();
    for (int i = position; i < limit; i++) {
      if (bs[i] != 0) {
        return false;
      }
    }
    return true;
  }

  private Operation getDeleteOperation(OperationType delete, long key) {
    Operation operation = new Operation();
    operation.type = delete;
    operation.key.set(key);
    return operation;
  }

  @Override
  public boolean get(List<ReadRequest> requests) throws IOException {
    ensureOpen();
    _readLock.lock();
    try {
      boolean moreRequestsNeeded = false;
      for (ReadRequest readRequest : requests) {
        Value internalValue = _pointers.get(readRequest.getBlockId());
        if (internalValue == null) {
          moreRequestsNeeded = true;
        } else {
          if (internalValue._value.getLength() == 0) {
            readRequest.handleEmptyResult();
          } else {
            readRequest.handleResult(internalValue._value.getBytes());
          }
        }
      }
      return moreRequestsNeeded;
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public boolean get(long key, ByteBuffer value) throws IOException {
    ensureOpen();
    _readLock.lock();
    try {
      Value internalValue = _pointers.get(key);
      if (internalValue == null) {
        return false;
      }
      value.put(internalValue._value.copyBytes());
      value.flip();
      return true;
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public void delete(long key) throws IOException {
    ensureOpen();
    _writeLock.lock();
    ensureOpenForWriting();
    try {
      Operation op = getDeleteOperation(OperationType.DELETE, key);
      write(op);
      Value old = _pointers.remove(key);
      if (old != null) {
        _size.addAndGet(-old._value.getCapacity());
      }
    } catch (RemoteException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } catch (LeaseExpiredException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    if (!_isClosed) {
      _isClosed = true;
      if (_idleLogTimerTask != null) {
        _idleLogTimerTask.cancel();
      }
      if (_oldFileCleanerTimerTask != null) {
        _oldFileCleanerTimerTask.cancel();
      }
      if (_hdfsKeyValueTimer != null) {
        _hdfsKeyValueTimer.purge();
      }
      _writeLock.lock();
      try {
        if (isOpenForWriting()) {
          try {
            syncInternal();
          } finally {
            IOUtils.closeQuietly(_output);
            _output = null;
          }
        }
      } finally {
        _writeLock.unlock();
      }
      _service.shutdownNow();
    }
  }

  private void openWriter() throws IOException {
    if (_readOnly) {
      throw new IOException("Key value store is set in read only mode.");
    }
    _outputPath = getSegmentPath(_currentFileCounter.incrementAndGet());
    _logger.info("Opening for writing {}", _outputPath);
    _output = _fileSystem.create(_outputPath, false);
    _output.write(MAGIC);
    _output.writeInt(VERSION);
    syncInternal();
  }

  private Path getSegmentPath(long segment) {
    Path path = new Path(_path, buffer(segment));
    return path.makeQualified(_fileSystem.getUri(), _fileSystem.getWorkingDirectory());
  }

  private static String buffer(long number) {
    String s = Long.toString(number);
    StringBuilder builder = new StringBuilder();
    for (int i = s.length(); i < 12; i++) {
      builder.append('0');
    }
    return builder.append(s)
                  .toString();
  }

  private void loadIndexes() throws IOException {
    for (FileStatus fileStatus : _fileStatus.get()) {
      loadIndex(fileStatus.getPath());
    }
  }

  private void ensureOpen() throws IOException {
    if (_isClosed) {
      throw new IOException("Already closed.");
    }
  }

  private void syncInternal() throws IOException {
    validateNextSegmentHasNotStarted();
    _output.hsync();
    _lastWrite.set(System.currentTimeMillis());
  }

  private void flushInternal() throws IOException {
    validateNextSegmentHasNotStarted();
    _output.hflush();
    _lastWrite.set(System.currentTimeMillis());
  }

  private void validateNextSegmentHasNotStarted() throws IOException {
    if (!isOwner()) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.");
    }
  }

  private void loadIndex(Path path) throws IOException {
    FSDataInputStream inputStream = _fileSystem.open(path);
    byte[] buf = new byte[MAGIC.length];
    inputStream.readFully(buf);
    if (!Arrays.equals(MAGIC, buf)) {
      throw new IOException("File [" + path + "] not a " + HDFS_KEY_VALUE + " file.");
    }
    int version = inputStream.readInt();
    if (version == 1) {
      long fileLength = HdfsUtils.getFileLength(_fileSystem, path, inputStream);
      Operation operation = new Operation();
      try {
        while (inputStream.getPos() < fileLength) {
          try {
            operation.readFields(inputStream);
          } catch (IOException e) {
            // End of sync point found
            return;
          }
          loadIndex(path, operation);
        }
      } finally {
        inputStream.close();
      }
    } else {
      throw new IOException("Unknown version [" + version + "]");
    }
  }

  private void loadIndex(Path path, Operation operation) {
    Value old;
    BytesWritable value = copy(operation.value);
    long key = operation.key.get();
    switch (operation.type) {
    case PUT:
      _size.addAndGet(value.getCapacity());
      old = _pointers.put(key, new Value(value, path));
      break;
    case DELETE:
      old = _pointers.remove(key);
      break;
    default:
      throw new RuntimeException("Not supported " + operation.type + "");
    }
    if (old != null) {
      _size.addAndGet(-old._value.getCapacity());
    }
  }

  private BytesWritable copy(BytesWritable value) {
    return new BytesWritable(value.copyBytes());
  }

  private SortedSet<FileStatus> getSortedSet(Path p) throws IOException {
    if (_fileSystem.exists(p)) {
      FileStatus[] listStatus = _fileSystem.listStatus(p);
      if (listStatus != null) {
        TreeSet<FileStatus> result = new TreeSet<FileStatus>();
        for (FileStatus fileStatus : listStatus) {
          if (!fileStatus.isDirectory()) {
            result.add(fileStatus);
          }
        }
        return result;
      }
    }
    return new TreeSet<FileStatus>();
  }

  @Override
  public boolean isOwner() throws IOException {
    Path p = getSegmentPath(_currentFileCounter.get() + 1);
    if (_fileSystem.exists(p)) {
      return false;
    }
    return true;
  }

  @Override
  public Iterable<Entry<Long, ByteBuffer>> scan(Long key) throws IOException {
    ensureOpen();
    NavigableMap<Long, Value> pointers = createSnapshot();
    return getIterable(key, pointers);
  }

  private Iterable<Entry<Long, ByteBuffer>> getIterable(Long key, NavigableMap<Long, Value> pointers) {
    if (key == null) {
      key = pointers.firstKey();
    }
    NavigableMap<Long, Value> tailMap = pointers.tailMap(key, true);
    return getIterable(tailMap);
  }

  private NavigableMap<Long, Value> createSnapshot() {
    _writeLock.lock();
    try {
      return new ConcurrentSkipListMap<Long, Value>(_pointers);
    } finally {
      _writeLock.unlock();
    }
  }

  private Iterable<Entry<Long, ByteBuffer>> getIterable(NavigableMap<Long, Value> map) {
    final Set<Entry<Long, Value>> entrySet = map.entrySet();
    return new Iterable<Entry<Long, ByteBuffer>>() {
      @Override
      public Iterator<Entry<Long, ByteBuffer>> iterator() {
        final Iterator<Entry<Long, Value>> iterator = entrySet.iterator();
        return new Iterator<Entry<Long, ByteBuffer>>() {

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Entry<Long, ByteBuffer> next() {
            final Entry<Long, Value> e = iterator.next();
            return new Entry<Long, ByteBuffer>() {

              @Override
              public ByteBuffer setValue(ByteBuffer value) {
                throw new RuntimeException("Read only.");
              }

              @Override
              public ByteBuffer getValue() {
                return ByteBuffer.wrap(e.getValue()._value.copyBytes());
              }

              @Override
              public Long getKey() {
                return e.getKey();
              }
            };
          }

          @Override
          public void remove() {
            throw new RuntimeException("Read only.");
          }
        };
      }
    };
  }

  @Override
  public void writeExternal(ExternalWriter externalWriter, boolean removeKeyValuesOnClose) throws IOException {
    final List<Entry<Long, Value>> entrySet = new ArrayList<>(_pointers.entrySet());
    for (Entry<Long, Value> entry : entrySet) {
      externalWriter.write(entry.getKey(), entry.getValue()._value);
    }
    externalWriter.commit();
    if (removeKeyValuesOnClose) {
      for (Entry<Long, Value> entry : entrySet) {
        Long key = entry.getKey();
        _writeLock.lock();
        Value value = _pointers.get(key);
        if (value == entry.getValue()) {
          Value remove = _pointers.remove(key);
          if (remove != null) {
            _size.addAndGet(-remove._value.getCapacity());
          }
        } else {
          _logger.info("Key {} was updated during write external.", key);
        }
        _writeLock.unlock();
      }
    }
  }

  @Override
  public long getSizeOfData() {
    return _size.get();
  }

  @Override
  public int getNumberOfEntries() {
    return _pointers.size();
  }

}