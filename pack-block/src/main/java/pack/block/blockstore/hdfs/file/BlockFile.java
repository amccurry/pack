package pack.block.blockstore.hdfs.file;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;

public class BlockFile {

  private static final String HDFS_BLOCK_FILE_V1 = "hdfs_block_file_v1";
  private static final String UTF_8 = "UTF-8";
  private final static byte[] MAGIC_STR;

  static {
    try {
      MAGIC_STR = HDFS_BLOCK_FILE_V1.getBytes(UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Writer create(FileSystem fileSystem, Path path, int blockSize) throws IOException {
    return new Writer(fileSystem, path, blockSize);
  }

  public static Reader open(FileSystem fileSystem, Path path) throws IOException {
    return new Reader(fileSystem, path);
  }

  public static void merge(List<Reader> readers, Writer writer) {
    List<PeekableIterator<BlockFileEntry>> iteratorList = new ArrayList<>();
    for (Reader reader : readers) {
      PeekableIterator<BlockFileEntry> iterator = PeekableIterator.wrap(reader.iterator());
      iteratorList.add(iterator);
    }
    
    // need to round robin through the readers...

  }

  public static class Writer implements Closeable {

    private final RoaringBitmap _blocks = new RoaringBitmap();
    private final RoaringBitmap _emptyBlocks = new RoaringBitmap();
    private final FSDataOutputStream _output;
    private final int _blockSize;

    private long _prevKey = Long.MIN_VALUE;

    private Writer(FileSystem fileSystem, Path path, int blockSize) throws IOException {
      _output = fileSystem.create(path);
      _blockSize = blockSize;
    }

    public void append(long longKey, BytesWritable value) throws IOException {
      int key = checkKey(longKey);
      checkValue(value, _blockSize);
      if (isValueAllZeros(value) || value.getLength() == 0) {
        _emptyBlocks.add(key);
      } else {
        _blocks.add(key);
        _output.write(value.getBytes(), 0, value.getLength());
      }
      _prevKey = longKey;
    }

    private boolean isValueAllZeros(BytesWritable value) {
      int length = value.getLength();
      byte[] bytes = value.getBytes();
      for (int i = 0; i < length; i++) {
        if (bytes[i] != 0) {
          return false;
        }
      }
      return true;
    }

    private int checkKey(long key) throws IOException {
      if (key <= _prevKey) {
        throw new IOException("Key " + key + " is less then or equal to prevkey " + _prevKey);
      }
      return getIntKey(key);
    }

    @Override
    public void close() throws IOException {
      long pos = _output.getPos();
      _blocks.serialize(_output);
      _emptyBlocks.serialize(_output);
      _output.writeInt(_blockSize);
      _output.writeInt(MAGIC_STR.length);
      _output.write(MAGIC_STR);
      _output.writeLong(pos);
      _output.close();
    }

  }

  public static class Reader implements Iterable<BlockFileEntry>, Closeable {

    private final RoaringBitmap _blocks = new RoaringBitmap();
    private final RoaringBitmap _emptyBlocks = new RoaringBitmap();
    private final FSDataInputStream _inputStream;
    private final int _blockSize;

    private Reader(FileSystem fileSystem, Path path) throws IOException {
      _inputStream = fileSystem.open(path);
      FileStatus fileStatus = fileSystem.getFileStatus(path);
      long len = fileStatus.getLen();
      _inputStream.seek(len - 8);
      long metaDataPosition = _inputStream.readLong();
      _inputStream.seek(metaDataPosition);
      _blocks.deserialize(_inputStream);
      _emptyBlocks.deserialize(_inputStream);
      _blockSize = _inputStream.readInt();
      // @TODO read and validate the magic str
    }

    public boolean read(long longKey, BytesWritable value) throws IOException {
      int key = getIntKey(longKey);
      value.setSize(_blockSize);
      if (_emptyBlocks.contains(key)) {
        setAllZeros(value);
        return true;
      }
      if (!_blocks.contains(key)) {
        return false;
      }
      readBlock(key, value);
      return true;
    }

    private void readBlock(int key, BytesWritable value) throws IOException {
      int storageBlockPosition = _blocks.rank(key) - 1;
      long position = storageBlockPosition * (long) _blockSize;
      _inputStream.read(position, value.getBytes(), 0, _blockSize);
    }

    private void setAllZeros(BytesWritable value) {
      int length = value.getLength();
      Arrays.fill(value.getBytes(), 0, length, (byte) 0);
    }

    @Override
    public void close() throws IOException {
      _inputStream.close();
    }

    @Override
    public Iterator<BlockFileEntry> iterator() {
      PeekableIterator<Integer> emptyIterator = PeekableIterator.wrap(_emptyBlocks.iterator());
      PeekableIterator<Integer> blocksIterator = PeekableIterator.wrap(_blocks.iterator());
      return newIndexIterator(emptyIterator, blocksIterator);
    }

    private Iterator<BlockFileEntry> newIndexIterator(PeekableIterator<Integer> emptyIterator,
        PeekableIterator<Integer> blocksIterator) {
      return new Iterator<BlockFileEntry>() {

        @Override
        public boolean hasNext() {
          if (emptyIterator.peek() != null) {
            return true;
          } else if (blocksIterator.peek() != null) {
            return true;
          } else {
            return false;
          }
        }

        @Override
        public BlockFileEntry next() {
          Integer e = emptyIterator.peek();
          Integer b = blocksIterator.peek();
          if (e.compareTo(b) < 0) {
            int id = emptyIterator.next();
            return newBlockFileEntry(id);
          } else {
            int id = blocksIterator.next();
            return newBlockFileEntry(id);
          }
        }

      };
    }

    protected Long toLong(Integer i) {
      return (long) ((int) i);
    }

    private BlockFileEntry newBlockFileEntry(int id) {
      return new BlockFileEntry() {

        private BytesWritable value;

        @Override
        public long getBlockId() {
          return id;
        }

        @Override
        public boolean isEmpty() {
          return true;
        }

        @Override
        public BytesWritable getData() throws IOException {
          if (value == null) {
            value = new BytesWritable();
            readBlock(id, value);
          }
          return value;
        }
      };
    }

  }

  private static int getIntKey(long key) throws IOException {
    if (key < Integer.MAX_VALUE) {
      return (int) key;
    }
    throw new IOException("Key " + key + " is too large >= " + Integer.MAX_VALUE);
  }

  private static void checkValue(BytesWritable value, int blockSize) throws IOException {
    if (value.getLength() > 0 && value.getLength() != blockSize) {
      throw new IOException("Value size " + value.getLength() + " is not equal to block size " + blockSize);
    }
  }

  public static interface BlockFileEntry {

    long getBlockId();

    boolean isEmpty();

    BytesWritable getData() throws IOException;

  }
}
