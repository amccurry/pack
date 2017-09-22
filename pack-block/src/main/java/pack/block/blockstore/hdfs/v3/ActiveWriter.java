package pack.block.blockstore.hdfs.v3;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.hdfs.file.BlockFile.Writer;
import pack.block.blockstore.hdfs.file.ReadRequest;
import pack.block.util.Utils;

public class ActiveWriter implements Closeable {

  private final static Logger LOGGER = LoggerFactory.getLogger(HdfsBlockStoreV3.class);

  private static final ByteBuffer EMPTY_BLOCK = ByteBuffer.allocate(0);

  private final Writer _writer;
  private final RoaringBitmap _index;
  private final AtomicLong _cacheSize = new AtomicLong();
  private final Map<Long, ByteBuffer> _cache = new ConcurrentHashMap<>();
  private final long _maxSize = 10_000_000;
  private final int _maxCap = 10_000;

  public ActiveWriter(Writer writer, RoaringBitmap index) {
    _writer = writer;
    _index = index;
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
    flush();
    Utils.time(LOGGER, "activeWriter.close", () -> {
      _writer.close();
      return null;
    });
  }

  public boolean contains(int key) {
    return _index.contains(key);
  }

  public void checkCache(List<ReadRequest> requests) {
    for (ReadRequest request : requests) {
      long blockId = request.getBlockId();
      ByteBuffer byteBuffer = _cache.get(blockId);
      if (byteBuffer != null) {
        if (byteBuffer == EMPTY_BLOCK) {
          request.handleEmptyResult();
        } else {
          request.handleResult(byteBuffer.duplicate());
        }
      }
    }
  }

  private ByteBuffer copy(ByteBuffer byteBuffer) {
    // this method may be over kill.
    ByteBuffer buffer = ByteBuffer.allocate(byteBuffer.remaining());
    buffer.put(byteBuffer);
    buffer.flip();
    return buffer;
  }

  public static void recoverBlock(FileSystem fileSystem, Path path) {

  }
}