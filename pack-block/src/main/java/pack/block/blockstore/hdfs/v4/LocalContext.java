package pack.block.blockstore.hdfs.v4;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.hdfs.file.ReadRequest;
import pack.block.blockstore.hdfs.file.WalKeyWritable;
import pack.block.blockstore.hdfs.file.WalKeyWritable.Type;
import pack.block.util.Utils;

public class LocalContext implements Closeable {

  private static final String RW = "rw";

  private final static Logger LOGGER = LoggerFactory.getLogger(LocalContext.class);

  private final RoaringBitmap _dataIndex = new RoaringBitmap();
  private final RoaringBitmap _emptyIndex = new RoaringBitmap();
  private final File _file;
  private final RandomAccessFile _rnd;
  private final int _blockSize;
  private final FileChannel _channel;

  public static void applyWal(FileSystem fileSystem, Path path, LocalContext localContext) throws IOException {
    try (SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem.getConf(), SequenceFile.Reader.file(path))) {
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

  public LocalContext(File file, long length, int blockSize) throws IOException {
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
    _dataIndex.add(id);
    _emptyIndex.remove(id);
    long pos = blockId * _blockSize;
    while (byteBuffer.remaining() > 0) {
      int write = _channel.write(byteBuffer, pos);
      pos += write;
    }
  }

  @Override
  public void close() throws IOException {
    Utils.close(LOGGER, _channel);
    Utils.close(LOGGER, _rnd);
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
}
