package pack.iscsi.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import pack.block.util.Utils;

public class WalCache implements BlockReader {

  private static final String RW = "rw";

  private final static Logger LOGGER = LoggerFactory.getLogger(WalCache.class);

  private final RoaringBitmap _dataIndex = new RoaringBitmap();
  private final RoaringBitmap _emptyIndex = new RoaringBitmap();
  private final File _file;
  private final RandomAccessFile _rnd;
  private final int _blockSize;
  private final FileChannel _channel;
  private final Cache<Integer, byte[]> _cache;
  private final long _startingOffset;
  private final long _endingOffset;
  private final boolean _deleteFileOnClose;

  public WalCache(long startingOffset, int offsetLength, File file, long length, int blockSize, long cacheSize)
      throws IOException {
    this(startingOffset, offsetLength, file, length, blockSize, cacheSize, true);
  }

  public WalCache(long startingOffset, int offsetLength, File file, long length, int blockSize, long cacheSize,
      boolean deleteFileOnClose) throws IOException {
    _deleteFileOnClose = deleteFileOnClose;
    _startingOffset = startingOffset;
    _endingOffset = startingOffset + offsetLength;
    _blockSize = blockSize;
    _file = file;
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
                         .maximumWeight(cacheSize)
                         .weigher(weigher)
                         .build();
  }

  public long getEndingOffset() {
    return _endingOffset;
  }

  public long getStartingOffset() {
    return _startingOffset;
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
    if (_deleteFileOnClose) {
      _file.delete();
    }
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
    return "PackWalCache [file=" + _file + "]";
  }

}
