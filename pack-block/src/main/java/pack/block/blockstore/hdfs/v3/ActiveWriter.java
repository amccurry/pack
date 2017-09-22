package pack.block.blockstore.hdfs.v3;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.ReaderMultiOrdered;
import pack.block.blockstore.hdfs.file.BlockFile.WriterMultiOrdered;
import pack.block.blockstore.hdfs.file.ReadRequest;
import pack.block.util.Utils;

public class ActiveWriter implements Closeable {

  private final static Logger LOGGER = LoggerFactory.getLogger(HdfsBlockStoreV3.class);

  private static final ByteBuffer EMPTY_BLOCK = ByteBuffer.allocate(0);

  private final WriterMultiOrdered _writer;
  private final RoaringBitmap _index;
  private final AtomicLong _cacheSize = new AtomicLong();
  private final Map<Long, ByteBuffer> _cache = new ConcurrentHashMap<>();
  private final long _maxSize = 10_000_000;
  private final int _maxCap = 10_000;
  // private final Thread _readerThread;
  private final Path _path;
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final AtomicReference<ReaderMultiOrdered> _reader = new AtomicReference<>();
  private final FileSystem _fileSystem;
  private final Object _readLock = new Object();
  private final AtomicInteger _commitCount = new AtomicInteger();
  private final int _maxCommitCount;

  // private final BlockingQueue<SyncIndex> _readPositionQueue = new
  // ArrayBlockingQueue<>(1);

  public ActiveWriter(FileSystem fileSystem, WriterMultiOrdered writer, RoaringBitmap index, Path path,
      int maxCommitCount) {
    _maxCommitCount = maxCommitCount;
    _fileSystem = fileSystem;
    _writer = writer;
    _index = index;
    _path = path;
    // _readerThread = createReaderThread();
    // _readerThread.start();
  }

  public static void recoverBlock(FileSystem fileSystem, Path path) {

  }

  private void doReaderReOpen(SyncIndex syncIndex) throws IOException {
    synchronized (_readLock) {
      long position = syncIndex._syncPosition;
      ReaderMultiOrdered reader = _reader.get();
      if (reader == null) {
        _reader.set(BlockFile.openMultiOrdered(_fileSystem, _path, position));
      } else {
        _reader.set(reader.reopen(_fileSystem, position));
        Utils.close(LOGGER, reader);
      }
    }
  }

  public void append(long blockId, ByteBuffer buffer) throws IOException {
    _index.add(Utils.getIntKey(blockId));
    ByteBuffer copy = copy(buffer);
    _cache.put(blockId, copy);
    _cacheSize.addAndGet(copy.capacity());
    flushIfNeeded();
  }

  public void flushIfNeeded() throws IOException {
    if (_cacheSize.get() >= _maxSize || _cache.size() > _maxCap) {
      flush();
    }
  }

  public void flush() throws IOException {
    if (_cache.isEmpty()) {
      return;
    }
    LOGGER.info("flush size {} count {}", _cacheSize.get(), _cache.size());
    List<Long> blockIds = new ArrayList<>(_cache.keySet());
    Collections.sort(blockIds);
    for (Long blockId : blockIds) {
      ByteBuffer byteBuffer = _cache.get(blockId);
      if (byteBuffer == EMPTY_BLOCK) {
        _writer.appendEmpty(blockId);
      } else {
        _writer.append(blockId, Utils.toBw(byteBuffer));
      }
    }
    _cache.clear();
    _cacheSize.set(0);
  }

  public void appendEmpty(long blockId) throws IOException {
    _index.add(Utils.getIntKey(blockId));
    _cache.put(blockId, EMPTY_BLOCK);
    flushIfNeeded();
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    // _readerThread.interrupt();
    // try {
    // _readerThread.join();
    // } catch (InterruptedException e) {
    // LOGGER.error("Unknown error", e);
    // }
    flush();
    Utils.time(LOGGER, "activeWriter.close", () -> {
      _writer.close();
      return null;
    });
  }

  public boolean contains(int key) {
    return _index.contains(key);
  }

  public boolean checkCurrentWriteLog(List<ReadRequest> requests) throws IOException {
    if (waitUntilDataIsVisible(requests)) {
      synchronized (_readLock) {
        ReaderMultiOrdered reader = _reader.get();
        if (reader != null) {
          return reader.read(requests);
        }
      }
    }
    return true;
  }

  private boolean waitUntilDataIsVisible(List<ReadRequest> requests) {
    return true;
  }

  // private boolean waitUntilDataIsVisible(List<ReadRequest> requests) throws
  // IOException {
  // SyncIndex syncIndex = _readPositionQueue.peek();
  // if (syncIndex == null) {
  // return false;
  // }
  // RoaringBitmap index = syncIndex._index;
  // for (ReadRequest request : requests) {
  // if (index.contains(Utils.getIntKey(request.getBlockId()))) {
  // while (true) {
  // SyncIndex peek = _readPositionQueue.peek();
  // if (peek == null || peek != syncIndex) {
  // return true;
  // }
  // sleep();
  // }
  // }
  // }
  // return false;
  // }

  public boolean checkCache(List<ReadRequest> requests) {
    boolean more = false;
    for (ReadRequest request : requests) {
      long blockId = request.getBlockId();
      ByteBuffer byteBuffer = _cache.get(blockId);
      if (byteBuffer != null) {
        if (byteBuffer == EMPTY_BLOCK) {
          request.handleEmptyResult();
        } else {
          request.handleResult(byteBuffer.duplicate());
        }
      } else {
        more = true;
      }
    }
    return more;
  }

  private ByteBuffer copy(ByteBuffer byteBuffer) {
    // this method may be over kill.
    ByteBuffer buffer = ByteBuffer.allocate(byteBuffer.remaining());
    buffer.put(byteBuffer);
    buffer.flip();
    return buffer;
  }

  public boolean commit() throws IOException {
    RoaringBitmap index = _index.clone();
    flush();
    _writer.writeFooter();
    long syncPosition = _writer.sync();
    reopenReaderToNewPosition(syncPosition, index);
    return _commitCount.incrementAndGet() >= _maxCommitCount;
  }

  static class SyncIndex {
    final long _syncPosition;
    final RoaringBitmap _index;

    SyncIndex(long syncPosition, RoaringBitmap index) {
      _syncPosition = syncPosition;
      _index = index;
    }
  }

  private void reopenReaderToNewPosition(long syncPosition, RoaringBitmap index) throws IOException {
    // try {
    // _readPositionQueue.put(new SyncIndex(syncPosition, index));
    // } catch (InterruptedException e) {
    // throw new IOException(e);
    // }
    doReaderReOpen(new SyncIndex(syncPosition, index));
  }

  // private Runnable getRunnable() {
  // return () -> {
  // while (_running.get()) {
  // SyncIndex syncIndex = _readPositionQueue.peek();
  // if (syncIndex != null) {
  // try {
  // doReaderReOpen(syncIndex);
  // try {
  // _readPositionQueue.take();
  // } catch (InterruptedException e) {
  // return;
  // }
  // } catch (IOException e) {
  // LOGGER.error("Unknown error while trying to reopen write log.", e);
  // }
  // } else {
  // sleep();
  // }
  // }
  // };
  // }
  //
  // private static void sleep() {
  // try {
  // Thread.sleep(TimeUnit.SECONDS.toMillis(400));
  // } catch (InterruptedException e) {
  // return;
  // }
  // }
  //
  // private Thread createReaderThread() {
  // Thread thread = new Thread(getRunnable());
  // thread.setDaemon(true);
  // thread.setName("reader-reopen-" + _path);
  // return thread;
  // }

}