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
package pack.distributed.storage.hdfs.kvs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.hdfs.HdfsUtils;

public class HdfsKeyValueStore implements Store {

  public static final int DEFAULT_MAX_AMOUNT_ALLOWED_PER_FILE = 64 * 1024 * 1024;
  public static final long DEFAULT_MAX_OPEN_FOR_WRITING = TimeUnit.MINUTES.toMillis(1);

  private static final String UTF_8 = "UTF-8";
  private static final String BLUR_KEY_VALUE = "blur_key_value";
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsKeyValueStore.class);
  private static final byte[] MAGIC;
  private static final int VERSION = 1;
  private static final long DAEMON_POLL_TIME = TimeUnit.SECONDS.toMillis(5);
  private static final int VERSION_LENGTH = 4;

  static {
    try {
      MAGIC = BLUR_KEY_VALUE.getBytes(UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  static enum OperationType {
    PUT {
      @Override
      public Operation createOperation(BytesRef key, BytesRef value) {
        Operation operation = new Operation();
        operation.type = OperationType.PUT;
        operation.key.set(key.bytes, key.offset, key.length);
        operation.value.set(value.bytes, value.offset, value.length);
        return operation;
      }
    },
    DELETE {
      @Override
      public Operation createOperation(BytesRef key, BytesRef value) {
        Operation operation = new Operation();
        operation.type = OperationType.DELETE;
        operation.key.set(key.bytes, key.offset, key.length);
        return operation;
      }
    },
    DELETE_RANGE {
      @Override
      public Operation createOperation(BytesRef fromInclusive, BytesRef toExclusive) {
        Operation operation = new Operation();
        operation.type = OperationType.DELETE_RANGE;
        operation.key.set(fromInclusive.bytes, fromInclusive.offset, fromInclusive.length);
        operation.value.set(toExclusive.bytes, toExclusive.offset, toExclusive.length);
        return operation;
      }
    };

    public abstract Operation createOperation(BytesRef key, BytesRef value);
  }

  static class Operation implements Writable {

    OperationType type;
    BytesWritable key = new BytesWritable();
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
      } else if (type == OperationType.DELETE_RANGE) {
        out.write(2);
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
      case 2:
        type = OperationType.DELETE_RANGE;
        key.readFields(in);
        value.readFields(in);
        return;
      default:
        throw new RuntimeException("Not supported [" + b + "]");
      }
    }

  }

  static class Value {
    Value(BytesRef bytesRef, Path path) {
      _bytesRef = bytesRef;
      _path = path;
    }

    BytesRef _bytesRef;
    Path _path;
  }

  private final ConcurrentNavigableMap<BytesRef, Value> _pointers = new ConcurrentSkipListMap<BytesRef, Value>();
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
  private final AtomicLong _lastSyncPosition = new AtomicLong();

  private FSDataOutputStream _output;
  private Path _outputPath;
  private boolean _isClosed;

  public HdfsKeyValueStore(boolean readOnly, Timer hdfsKeyValueTimer, Configuration configuration, Path path)
      throws IOException {
    this(readOnly, hdfsKeyValueTimer, configuration, path, DEFAULT_MAX_AMOUNT_ALLOWED_PER_FILE,
        DEFAULT_MAX_OPEN_FOR_WRITING);
  }

  public HdfsKeyValueStore(boolean readOnly, Timer hdfsKeyValueTimer, Configuration configuration, Path path,
      long maxAmountAllowedPerFile) throws IOException {
    this(readOnly, hdfsKeyValueTimer, configuration, path, maxAmountAllowedPerFile, DEFAULT_MAX_OPEN_FOR_WRITING);
  }

  public HdfsKeyValueStore(boolean readOnly, Timer hdfsKeyValueTimer, Configuration configuration, Path path,
      long maxAmountAllowedPerFile, long maxTimeOpenForWriting) throws IOException {
    _readOnly = readOnly;
    _maxTimeOpenForWriting = maxTimeOpenForWriting;
    _maxAmountAllowedPerFile = maxAmountAllowedPerFile;
    _fileSystem = path.getFileSystem(configuration);
    _path = path.makeQualified(_fileSystem.getUri(), _fileSystem.getWorkingDirectory());
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
        LOGGER.warn("Removing file [{}] because length of [{}] is less than MAGIC plus version length of [{}]", path,
            len, MAGIC.length + VERSION_LENGTH);
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
          LOGGER.error("Unknown error while trying to clean up old files.", e);
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
          LOGGER.error("Unknown error while trying to close output file.", e);
        }
      }
    };
  }

  @Override
  public void sync(TransId transId) throws IOException {
    ensureOpen();
    _writeLock.lock();
    ensureOpenForWriting();
    try {
      syncInternal(transId);
    } catch (RemoteException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } catch (LeaseExpiredException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public Iterable<Entry<BytesRef, BytesRef>> scan(BytesRef key) throws IOException {
    ensureOpen();
    NavigableMap<BytesRef, Value> pointers = createSnapshot();
    return getIterable(key, pointers);
  }

  @Override
  public BytesRef lastKey() throws IOException {
    _writeLock.lock();
    try {
      return _pointers.lastKey();
    } finally {
      _writeLock.unlock();
    }
  }

  private Iterable<Entry<BytesRef, BytesRef>> getIterable(BytesRef key, NavigableMap<BytesRef, Value> pointers) {
    if (key == null) {
      key = pointers.firstKey();
    }
    NavigableMap<BytesRef, Value> tailMap = pointers.tailMap(key, true);
    return getIterable(tailMap);
  }

  private NavigableMap<BytesRef, Value> createSnapshot() {
    _writeLock.lock();
    try {
      return new ConcurrentSkipListMap<BytesRef, Value>(_pointers);
    } finally {
      _writeLock.unlock();
    }
  }

  private Iterable<Entry<BytesRef, BytesRef>> getIterable(NavigableMap<BytesRef, Value> map) {
    final Set<Entry<BytesRef, Value>> entrySet = map.entrySet();
    return new Iterable<Entry<BytesRef, BytesRef>>() {
      @Override
      public Iterator<Entry<BytesRef, BytesRef>> iterator() {
        final Iterator<Entry<BytesRef, Value>> iterator = entrySet.iterator();
        return new Iterator<Entry<BytesRef, BytesRef>>() {

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Entry<BytesRef, BytesRef> next() {
            final Entry<BytesRef, Value> e = iterator.next();
            return new Entry<BytesRef, BytesRef>() {

              @Override
              public BytesRef setValue(BytesRef value) {
                throw new RuntimeException("Read only.");
              }

              @Override
              public BytesRef getValue() {
                return e.getValue()._bytesRef;
              }

              @Override
              public BytesRef getKey() {
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
  public TransId put(BytesRef key, BytesRef value) throws IOException {
    ensureOpen();
    if (value == null) {
      return delete(key);
    }
    _writeLock.lock();
    ensureOpenForWriting();
    try {
      Operation op = OperationType.PUT.createOperation(key, value);
      TransId transId = write(op);
      doPut(key, value, transId.getPath());
      return transId;
    } catch (RemoteException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } catch (LeaseExpiredException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } finally {
      _writeLock.unlock();
    }
  }

  private void doPut(BytesRef key, BytesRef value, Path path) {
    BytesRef deepCopyOf = BytesRef.deepCopyOf(value);
    _size.addAndGet(deepCopyOf.bytes.length);
    Value old = _pointers.put(BytesRef.deepCopyOf(key), new Value(deepCopyOf, path));
    if (old != null) {
      _size.addAndGet(-old._bytesRef.bytes.length);
    }
  }

  private void ensureOpenForWriting() throws IOException {
    if (_output == null) {
      openWriter();
    }
  }

  private TransId write(Operation op) throws IOException {
    op.write(_output);
    Path p = _outputPath;
    long pos = _output.getPos();
    if (pos >= _maxAmountAllowedPerFile) {
      rollFile();
    }
    return TransId.builder()
                  .path(p)
                  .position(pos)
                  .build();
  }

  private void rollFile() throws IOException {
    LOGGER.info("Rolling file [{}]", _outputPath);
    _output.close();
    _output = null;
    _lastSyncPosition.set(0);
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
        throw new IOException(
            "No longer the owner of [" + _path + "] output [" + _outputPath + "] newestGen [" + newestGen + "]");
      }
      Set<Path> existingFiles = new HashSet<Path>();
      for (FileStatus fileStatus : fileStatusSet) {
        existingFiles.add(fileStatus.getPath());
      }
      Set<Entry<BytesRef, Value>> entrySet = _pointers.entrySet();
      existingFiles.remove(_outputPath);
      for (Entry<BytesRef, Value> e : entrySet) {
        Path p = e.getValue()._path;
        existingFiles.remove(p);
      }
      for (Path p : existingFiles) {
        LOGGER.info("Removing file no longer referenced [{}]", p);
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
        LOGGER.info("Closing KV log due to inactivity [{}].", _path);
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

  @Override
  public boolean get(BytesRef key, BytesRef value) throws IOException {
    ensureOpen();
    _readLock.lock();
    try {
      Value internalValue = _pointers.get(key);
      if (internalValue == null) {
        return false;
      }
      value.copyBytes(internalValue._bytesRef);
      return true;
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public TransId delete(BytesRef key) throws IOException {
    ensureOpen();
    _writeLock.lock();
    ensureOpenForWriting();
    try {
      Operation op = OperationType.DELETE.createOperation(key, null);
      TransId transId = write(op);
      doDelete(key);
      return transId;
    } catch (RemoteException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } catch (LeaseExpiredException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } finally {
      _writeLock.unlock();
    }
  }

  private void doDelete(BytesRef key) {
    Value old = _pointers.remove(key);
    if (old != null) {
      _size.addAndGet(-old._bytesRef.bytes.length);
    }
  }

  @Override
  public TransId deleteRange(BytesRef fromInclusive, BytesRef toExclusive) throws IOException {
    ensureOpen();
    _writeLock.lock();
    ensureOpenForWriting();
    try {
      Operation op = OperationType.DELETE_RANGE.createOperation(fromInclusive, toExclusive);
      TransId transId = write(op);
      doDeleteRange(fromInclusive, toExclusive);
      return transId;
    } catch (RemoteException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } catch (LeaseExpiredException e) {
      throw new IOException("Another HDFS KeyStore has taken ownership of this key value store.", e);
    } finally {
      _writeLock.unlock();
    }
  }

  private void doDeleteRange(BytesRef fromInclusive, BytesRef toExclusive) {
    for (Entry<BytesRef, Value> e : _pointers.entrySet()) {
      BytesRef key = e.getKey();
      if (key.compareTo(fromInclusive) >= 0 && key.compareTo(toExclusive) < 0) {
        doDelete(key);
      }
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
            syncInternal(null);
          } finally {
            IOUtils.closeQuietly(_output);
            _output = null;
          }
        }
      } finally {
        _writeLock.unlock();
      }
    }
  }

  private void openWriter() throws IOException {
    if (_readOnly) {
      throw new IOException("Key value store is set in read only mode.");
    }
    _outputPath = getSegmentPath(_currentFileCounter.incrementAndGet());
    LOGGER.info("Opening for writing [{}].", _outputPath);
    _output = _fileSystem.create(_outputPath, false);
    _output.write(MAGIC);
    _output.writeInt(VERSION);
    syncInternal(null);
  }

  private Path getSegmentPath(long segment) {
    return new Path(_path, buffer(segment));
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

  private void syncInternal(TransId transId) throws IOException {
    if (!needsToSync(transId)) {
      // Another thread performed a sync that covered this transaction.
      LOGGER.debug("sync not needed");
      return;
    }
    validateNextSegmentHasNotStarted();
    long pos = _output.getPos();
    _output.hflush();
    _lastSyncPosition.set(pos);
    _lastWrite.set(System.currentTimeMillis());
  }

  private boolean needsToSync(TransId transId) {
    if (transId == null) {
      return true;
    }
    if (!_outputPath.equals(transId.getPath())) {
      // Log has rolled, and it syncs during roll.
      return false;
    } else if (_lastSyncPosition.get() >= transId.getPosition()) {
      return false;
    } else {
      return true;
    }
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
      throw new IOException("File [" + path + "] not a " + BLUR_KEY_VALUE + " file.");
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
    switch (operation.type) {
    case PUT:
      doPut(getKey(operation.key), getKey(operation.value), path);
      break;
    case DELETE:
      doDelete(getKey(operation.key));
      break;
    case DELETE_RANGE:
      doDeleteRange(getKey(operation.key), getKey(operation.value));
      break;
    default:
      throw new RuntimeException("Not supported [" + operation.type + "]");
    }
  }

  private BytesRef getKey(BytesWritable key) {
    return new BytesRef(key.getBytes(), 0, key.getLength());
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

}