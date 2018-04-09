package pack.distributed.storage.walcache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import pack.distributed.storage.hdfs.BlockFile.Writer;
import pack.distributed.storage.read.ReadRequest;
import pack.iscsi.storage.utils.PackUtils;

public class PackWalCache implements WalCache {

  private static final String RW = "rw";

  private final static Logger LOGGER = LoggerFactory.getLogger(PackWalCache.class);

  private final RoaringBitmap _dataIndex = new RoaringBitmap();
  private final File _file;
  private final RandomAccessFile _rnd;
  private final int _blockSize;
  private final FileChannel _channel;
  private final boolean _deleteFileOnClose;
  private final Object _dataIndexLock = new Object();
  private final long _id;
  private final long _created = System.currentTimeMillis();
  private final AtomicLong _layer = new AtomicLong(-1L);
  private final Cache<Integer, byte[]> _cache;
  private final AtomicInteger _count = new AtomicInteger();
  private final AtomicBoolean _closed = new AtomicBoolean(false);
  private final AtomicInteger _ref = new AtomicInteger();

  public PackWalCache(File dirFile, long startingLayer, long length, int blockSize) throws IOException {
    this(dirFile, startingLayer, length, blockSize, true);
  }

  public PackWalCache(File dirFile, long startingLayer, long length, int blockSize, boolean deleteFileOnClose)
      throws IOException {
    _deleteFileOnClose = deleteFileOnClose;
    _blockSize = blockSize;
    _id = startingLayer;
    _layer.set(startingLayer);
    _file = new File(dirFile, Long.toString(_id));
    if (_file.exists()) {
      _file.delete();
    }
    _file.getParentFile()
         .mkdirs();
    _rnd = new RandomAccessFile(_file, RW);
    _rnd.setLength(length);
    _channel = _rnd.getChannel();
    Weigher<Integer, byte[]> weigher = (key, value) -> value.length;
    _cache = CacheBuilder.newBuilder()
                         .maximumWeight(blockSize * 32)
                         .weigher(weigher)
                         .concurrencyLevel(1)
                         .build();
  }

  @Override
  public int getSize() {
    return _count.get() * _blockSize;
  }

  public long getMaxLayer() {
    return _layer.get();
  }

  public boolean readBlocks(List<ReadRequest> requests) throws IOException {
    boolean more = false;
    for (ReadRequest readRequest : requests) {
      if (readBlock(readRequest)) {
        more = true;
      }
    }
    return more;
  }

  public boolean readBlock(ReadRequest readRequest) throws IOException {
    ensureOpen();
    int id = readRequest.getBlockId();
    if (_cache != null) {
      byte[] bs = _cache.getIfPresent(id);
      if (bs != null) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("wal read blockId {} md5 {}", id, PackUtils.toMd5(bs));
        }
        readRequest.handleResult(bs);
        return false;
      }
    }
    if (contains(id)) {
      ByteBuffer src = ByteBuffer.allocate(_blockSize);
      int blockId = readRequest.getBlockId();
      long pos = PackUtils.getPosition(blockId, _blockSize);
      int remaining = readRequest.getByteBuffer()
                                 .remaining();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("read bo {} bid {} rlen {} pos {}", readRequest.getBlockOffset(), blockId, remaining, pos);
      }
      while (src.remaining() > 0) {
        int read = _channel.read(src, pos);
        pos += read;
      }
      src.flip();
      readRequest.handleResult(src);
      return false;
    } else {
      return true;
    }
  }

  private boolean contains(int id) {
    synchronized (_dataIndexLock) {
      return _dataIndex.contains(id);
    }
  }

  private void add(int id) {
    synchronized (_dataIndexLock) {
      if (!_dataIndex.contains(id)) {
        _dataIndex.add(id);
        _count.incrementAndGet();
      }
    }
  }

  @Override
  public void write(long layer, int blockId, byte[] value) throws IOException {
    ensureOpen();
    add(blockId);
    if (_cache != null) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("wal write blockId {} md5 {}", blockId, PackUtils.toMd5(value));
      }
      _cache.put(blockId, PackUtils.copy(value, 0, value.length));
    }
    long pos = PackUtils.getPosition(blockId, _blockSize);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("write bo {} bid {} rlen {} pos {} layer {}", 0, blockId, value.length, pos, layer);
    }
    ByteBuffer byteBuffer = ByteBuffer.wrap(value);
    while (byteBuffer.remaining() > 0) {
      int write = _channel.write(byteBuffer, pos);
      pos += write;
    }
    setMaxLayer(layer);
  }

  private void setMaxLayer(long layer) {
    if (_layer.get() < layer) {
      _layer.set(layer);
    }
  }

  public void ensureOpen() throws IOException {
    if (isClosed()) {
      LOGGER.info("Wal file already close {}", _file);
      throw new IOException("Already closed");
    }
  }

  @Override
  public boolean isClosed() {
    return _closed.get();
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("Closing wal file {} max layer {}", _file, _layer.get());
    _closed.set(true);
    PackUtils.close(LOGGER, _channel);
    PackUtils.close(LOGGER, _rnd);
    if (_deleteFileOnClose) {
      _file.delete();
    }
  }

  public static ByteBuffer toBuffer(BytesWritable value) {
    return ByteBuffer.wrap(value.getBytes(), 0, value.getLength());
  }

  @Override
  public String toString() {
    return "WalCache [file=" + _file + "]";
  }

  @Override
  public long getId() {
    return _id;
  }

  public long getCreationTime() {
    return _created;
  }

  @Override
  public void copy(Writer writer) throws IOException {
    byte[] buf = new byte[_blockSize];
    synchronized (_dataIndex) {
      for (Integer id : _dataIndex) {
        long pos = PackUtils.getPosition(id, _blockSize);
        _rnd.seek(pos);
        _rnd.readFully(buf, 0, _blockSize);
        writer.append(id, new BytesWritable(buf));
      }
    }
  }

  @Override
  public void incRef() {
    _ref.incrementAndGet();
  }

  @Override
  public void decRef() {
    _ref.decrementAndGet();
  }

  @Override
  public int refCount() {
    return _ref.get();
  }

}
