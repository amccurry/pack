package pack.block.blockstore.hdfs.blockstore.wal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.google.common.base.Splitter;

import pack.block.blockstore.crc.CrcLayer;
import pack.block.blockstore.crc.CrcLayerFactory;
import pack.block.blockstore.hdfs.ReadRequestHandler;
import pack.block.blockstore.hdfs.blockstore.wal.WalFile.Reader;
import pack.block.blockstore.hdfs.blockstore.wal.WalKeyWritable.Type;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.ReadRequest;
import pack.block.util.Utils;
import pack.util.ExecUtil;
import pack.util.Result;

public class LocalWalCache implements ReadRequestHandler, Closeable {

  private static final String LOCAL_WAL_CACHE_DATA_MISSING = "LocalWalCache data missing";

  private static final String LOCAL_WAL_CACHE_DATA_READ = "LocalWalCache data read";

  private static final String RW = "rw";

  private final static Logger LOGGER = LoggerFactory.getLogger(LocalWalCache.class);

  private final RoaringBitmap _dataIndex = new RoaringBitmap();
  private final RoaringBitmap _emptyIndex = new RoaringBitmap();
  private final File _file;
  private final int _blockSize;
  private final AtomicLong _length;
  private final AtomicLong _currentLength = new AtomicLong();
  private final CrcLayer _crc;
  private final long _layer;
  private final AtomicReference<RandomAccessFile> _rnd = new AtomicReference<RandomAccessFile>();
  private final AtomicReference<FileChannel> _channel = new AtomicReference<FileChannel>();
  private final WriteLock _writeLock;
  private final ReadLock _readLock;

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
    this(file, length, blockSize, -1L);
  }

  public LocalWalCache(File file, AtomicLong length, int blockSize, long layer) throws IOException {
    _length = length;
    _blockSize = blockSize;
    _file = file;
    if (_file.exists()) {
      _file.delete();
    }
    _file.getParentFile()
         .mkdirs();
    if (layer < 0) {
      _layer = BlockFile.getLayer(_file);
    } else {
      _layer = layer;
    }
    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeLock = reentrantReadWriteLock.writeLock();
    _readLock = reentrantReadWriteLock.readLock();
    updateLength();
    _crc = CrcLayerFactory.create(file.getName(), (int) (length.get() / blockSize), blockSize);
  }

  @Override
  public long getLayer() {
    return _layer;
  }

  public void delete(long startingBlockId, long endingBlockId) throws IOException {
    _emptyIndex.add(startingBlockId, endingBlockId);
    _dataIndex.remove(startingBlockId, endingBlockId);
    _crc.delete((int) startingBlockId, (int) endingBlockId);
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
    _readLock.lock();
    try {
      int id = Utils.getIntKey(readRequest.getBlockId());
      if (_dataIndex.contains(id)) {
        ByteBuffer src = ByteBuffer.allocate(_blockSize);
        long blockId = readRequest.getBlockId();
        long pos = blockId * (long) _blockSize;
        while (src.remaining() > 0) {
          int read = _channel.get()
                             .read(src, pos);
          pos += read;
        }
        src.flip();
        _crc.validate(LOCAL_WAL_CACHE_DATA_READ, id, src.array());
        readRequest.handleResult(src);
        return false;
      } else if (_emptyIndex.contains(id)) {
        _crc.validateDeleted(LOCAL_WAL_CACHE_DATA_MISSING, id);
        readRequest.handleEmptyResult();
        return false;
      } else {
        return true;
      }
    } finally {
      _readLock.unlock();
    }
  }

  public void write(long blockId, ByteBuffer byteBuffer) throws IOException {
    updateLength();
    _readLock.lock();
    try {
      int id = Utils.getIntKey(blockId);
      _crc.put(id, byteBuffer);
      _dataIndex.add(id);
      _emptyIndex.remove(id);
      long pos = blockId * _blockSize;
      while (byteBuffer.remaining() > 0) {
        int write = _channel.get()
                            .write(byteBuffer, pos);
        pos += write;
      }
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    closeFile();
    Utils.close(LOGGER, _crc);
    _file.delete();
  }

  private void closeFile() {
    Utils.close(LOGGER, _channel.get());
    Utils.close(LOGGER, _rnd.get());
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

  private void updateLength() throws IOException {
    long l = _length.get();
    long currentLength = _currentLength.get();
    if (currentLength != l || _rnd.get() == null) {
      _writeLock.lock();
      try {
        _currentLength.set(l);
        LOGGER.info("Updating length of WAL file {} from {} to {}", _file, currentLength, l);
        closeFile();
        ExecUtil.exec(LOGGER, Level.INFO, "dd", "if=/dev/zero", "of=" + _file.getAbsolutePath(), "bs=1", "count=0",
            "seek=" + l);
        RandomAccessFile rnd = new RandomAccessFile(_file, RW);
        _rnd.set(rnd);
        _channel.set(rnd.getChannel());
      } finally {
        _writeLock.unlock();
      }
    }
  }

  public long getSizeOnDisk() throws IOException {
    Result result = ExecUtil.execAsResult(LOGGER, Level.DEBUG, "ls", "-ls", _file.getAbsolutePath());
    List<String> list = Splitter.on(' ')
                                .splitToList(result.stdout.trim());
    return Long.parseLong(list.get(0));
  }

  public long getLength() {
    return _length.get();
  }

}
