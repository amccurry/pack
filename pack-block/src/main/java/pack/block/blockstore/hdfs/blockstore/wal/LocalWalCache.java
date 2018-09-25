package pack.block.blockstore.hdfs.blockstore.wal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import pack.block.blockstore.hdfs.blockstore.wal.WalFile.Reader;
import pack.block.blockstore.hdfs.blockstore.wal.WalKeyWritable.Type;
import pack.block.blockstore.hdfs.file.ReadRequest;
import pack.block.util.Utils;

public class LocalWalCache implements Closeable {

  private static final String RW = "rw";

  private final static Logger LOGGER = LoggerFactory.getLogger(LocalWalCache.class);

  private final RoaringBitmap _dataIndex = new RoaringBitmap();
  private final RoaringBitmap _emptyIndex = new RoaringBitmap();
  private final File _file;
  private final RandomAccessFile _rnd;
  private final int _blockSize;
  private final FileChannel _channel;
  private final Cache<Integer, byte[]> _cache;
  private final AtomicLong _length;
  private final AtomicLong _currentLength = new AtomicLong();

  public static void applyWal(WalFileFactory walFactory, Path path, LocalWalCache localContext) throws IOException {
    try (Reader reader = walFactory.open(path)) {
      WalKeyWritable key = new WalKeyWritable();
      BytesWritable value = new BytesWritable();
      while (reader.next(key, value)) {
        Type type = key.getType();
        switch (type) {
        case DATA:
          localContext.write(key.getStartingBlockId(), toBuffer(value));
          break;
        case TRIM:
          localContext.delete(key.getStartingBlockId(), key.getEndingBlockId());
          break;
        default:
          throw new IOException("Unknown wal key type " + type);
        }
      }
    }
  }

  public LocalWalCache(File file, AtomicLong length, int blockSize) throws IOException {
    this(file, length, blockSize, 1024 * 1024);
  }

  public LocalWalCache(File file, AtomicLong length, int blockSize, long cacheSize) throws IOException {
    _length = length;
    _blockSize = blockSize;
    _file = file;
    if (_file.exists()) {
      _file.delete();
    }
    _file.getParentFile()
         .mkdirs();
    _rnd = new RandomAccessFile(_file, RW);
    updateLength();
    _channel = _rnd.getChannel();
    Weigher<Integer, byte[]> weigher = (key, value) -> value.length;
    _cache = CacheBuilder.newBuilder()
                         .maximumWeight(cacheSize)
                         .weigher(weigher)
                         .build();
  }

  private void updateLength() throws IOException {
    long l = _length.get();
    if (_currentLength.get() != l) {
      _currentLength.set(l);
      _rnd.setLength(l);
    }
  }

  public void delete(long startingBlockId, long endingBlockId) throws IOException {
    _emptyIndex.add(startingBlockId, endingBlockId);
    _dataIndex.remove(startingBlockId, endingBlockId);
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
    int id = Utils.getIntKey(readRequest.getBlockId());
    if (_cache != null) {
      byte[] bs = _cache.getIfPresent(id);
      if (bs != null) {
        readRequest.handleResult(bs);
        return false;
      }
    }
    if (_dataIndex.contains(id)) {
      ByteBuffer src = ByteBuffer.allocate(_blockSize);
      long blockId = readRequest.getBlockId();
      long pos = blockId * _blockSize;
      while (src.remaining() > 0) {
        int read = _channel.read(src, pos);
        pos += read;
      }
      src.flip();
      readRequest.handleResult(src);
      return false;
    } else if (_emptyIndex.contains(id)) {
      readRequest.handleEmptyResult();
      return false;
    } else {
      return true;
    }
  }

  public void write(long blockId, ByteBuffer byteBuffer) throws IOException {
    updateLength();
    int id = Utils.getIntKey(blockId);
    if (_cache != null) {
      _cache.put(id, toByteArray(byteBuffer));
    }
    _dataIndex.add(id);
    _emptyIndex.remove(id);
    long pos = blockId * _blockSize;
    while (byteBuffer.remaining() > 0) {
      int write = _channel.write(byteBuffer, pos);
      pos += write;
    }
  }

  private byte[] toByteArray(ByteBuffer byteBuffer) {
    ByteBuffer duplicate = byteBuffer.duplicate();
    byte[] bs = new byte[duplicate.remaining()];
    duplicate.get(bs);
    return bs;
  }

  @Override
  public void close() throws IOException {
    Utils.close(LOGGER, _channel);
    Utils.close(LOGGER, _rnd);
    _file.delete();
  }

  public static ByteBuffer toBuffer(BytesWritable value) {
    return ByteBuffer.wrap(value.getBytes(), 0, value.getLength());
  }

  public RoaringBitmap getDataBlocks() {
    return _dataIndex;
  }

  public RoaringBitmap getEmptyBlocks() {
    return _emptyIndex;
  }

  @Override
  public String toString() {
    return "LocalWalCache [file=" + _file + "]";
  }
}