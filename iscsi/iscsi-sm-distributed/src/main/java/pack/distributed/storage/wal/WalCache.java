package pack.distributed.storage.wal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import pack.distributed.storage.BlockReader;
import pack.distributed.storage.hdfs.BlockFile.Writer;
import pack.distributed.storage.hdfs.ReadRequest;
import pack.iscsi.storage.utils.PackUtils;

public class WalCache implements Comparable<WalCache>, BlockReader {

  private static final String RW = "rw";

  private final static Logger LOGGER = LoggerFactory.getLogger(WalCache.class);

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

  public WalCache(File dirFile, long startingLayer, long length, int blockSize) throws IOException {
    this(dirFile, startingLayer, length, blockSize, true);
  }

  public WalCache(File dirFile, long startingLayer, long length, int blockSize, boolean deleteFileOnClose)
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
                         .build();
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
    int id = readRequest.getBlockId();
    if (_cache != null) {
      byte[] bs = _cache.getIfPresent(id);
      if (bs != null) {
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
      LOGGER.info("read bo {} bid {} rlen {} pos {}", readRequest.getBlockOffset(), blockId, remaining, pos);
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
      _dataIndex.add(id);
    }
  }

  public void write(long layer, int blockId, ByteBuffer byteBuffer) throws IOException {
    add(blockId);
    if (_cache != null) {
      _cache.put(blockId, toByteArray(byteBuffer));
    }
    long pos = PackUtils.getPosition(blockId, _blockSize);

    LOGGER.info("write bo {} bid {} rlen {} pos {} layer {}", 0, blockId, byteBuffer.remaining(), pos, layer);
    while (byteBuffer.remaining() > 0) {
      int write = _channel.write(byteBuffer, pos);
      pos += write;
    }
    setMaxLayer(layer);
  }

  private byte[] toByteArray(ByteBuffer byteBuffer) {
    ByteBuffer duplicate = byteBuffer.duplicate();
    byte[] bs = new byte[duplicate.remaining()];
    duplicate.get(bs);
    return bs;
  }

  private void setMaxLayer(long layer) {
    if (_layer.get() < layer) {
      _layer.set(layer);
    }
  }

  @Override
  public void close() throws IOException {
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
  public int compareTo(WalCache o) {
    return Long.compare(o._id, _id);
  }

  public long getCreationTime() {
    return _created;
  }

  public void copy(Writer writer) throws IOException {
    byte[] buf = new byte[_blockSize];
    for (Integer id : _dataIndex) {
      long pos = PackUtils.getPosition(id, _blockSize);
      _rnd.seek(pos);
      _rnd.readFully(buf, 0, _blockSize);
      writer.append(id, new BytesWritable(buf));
    }
  }

}
