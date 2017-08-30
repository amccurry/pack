package pack.block.blockstore.hdfs.file;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class BlockFile {

  private static final String PACK_STREAM_ONLY = "PACK_STREAM_ONLY";
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockFile.class);
  private static final String HDFS_BLOCK_FILE_V1 = "hdfs_block_file_v1";
  private static final String UTF_8 = "UTF-8";
  private static final byte[] MAGIC_STR;
  private static final boolean STREAM_ONLY;

  static {
    try {
      MAGIC_STR = HDFS_BLOCK_FILE_V1.getBytes(UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    STREAM_ONLY = System.getenv(PACK_STREAM_ONLY) != null;
  }

  public static Writer create(FileSystem fileSystem, Path path, int blockSize, List<String> sourceFileList)
      throws IOException {
    return new Writer(fileSystem, path, blockSize, sourceFileList);
  }

  public static Writer create(FileSystem fileSystem, Path path, int blockSize) throws IOException {
    return new Writer(fileSystem, path, blockSize, ImmutableList.of());
  }

  public static Reader open(FileSystem fileSystem, Path path) throws IOException {
    if (STREAM_ONLY) {
      return openForStreaming(fileSystem, path);
    }
    return new RandomAccessReader(fileSystem, path);
  }

  public static Reader openForStreaming(FileSystem fileSystem, Path path) throws IOException {
    return new StreamReader(fileSystem, path);
  }

  public static void merge(List<Reader> readers, Writer writer) {
    merge(readers, writer, null);
  }

  public static void merge(List<Reader> readers, Writer writer, RoaringBitmap blocksToIgnore) {
    RoaringBitmap allBlocks = getAllBlocks(readers);
    BytesWritable value = new BytesWritable();
    int readerCount = readers.size();

    long longCardinality = applyIgnoreBlocks(blocksToIgnore, allBlocks);

    IntConsumer consumer = new IntConsumer() {
      private long count = 0;
      private long last;
      private long delay = TimeUnit.SECONDS.toMillis(5);

      @Override
      public void accept(int blockId) {
        long now = System.currentTimeMillis();
        if (last + delay < now) {
          LOGGER.info("merge {}% complete, count {} total {}",
              ((long) (((double) count / (double) longCardinality) * 1000) / 10.0), count, longCardinality);
          last = System.currentTimeMillis();
        }
        processReaders(readers, readerCount, blockId, writer, value);
        count++;
      }
    };

    allBlocks.forEach(consumer);
  }

  private static long applyIgnoreBlocks(RoaringBitmap blocksToIgnore, RoaringBitmap allBlocks) {
    long longCardinality;
    if (blocksToIgnore != null) {
      long longCardinalityBeforeIgnore = allBlocks.getLongCardinality();
      LOGGER.info("All blocks cardinality before ignore {}", longCardinalityBeforeIgnore);
      allBlocks.andNot(blocksToIgnore);
      longCardinality = allBlocks.getLongCardinality();
      LOGGER.info("All blocks cardinality after ignore {}", longCardinality);
    } else {
      longCardinality = allBlocks.getLongCardinality();
    }
    return longCardinality;
  }

  private static void processReaders(List<Reader> readers, int readerCount, int blockId, Writer writer,
      BytesWritable value) {
    try {
      for (int r = 0; r < readerCount; r++) {
        Reader reader = readers.get(r);
        if (reader.hasBlock(blockId)) {
          reader.read(blockId, value);
          writer.append(blockId, value);
          return;
        } else if (reader.hasEmptyBlock(blockId)) {
          writer.appendEmpty(blockId);
          return;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static RoaringBitmap getAllBlocks(List<Reader> readers) {
    RoaringBitmap result = new RoaringBitmap();
    readers.forEach(reader -> {
      result.or(reader._blocks);
      result.or(reader._emptyBlocks);
    });
    return result;
  }

  public static class Writer implements Closeable {

    private final RoaringBitmap _blocks = new RoaringBitmap();
    private final RoaringBitmap _emptyBlocks = new RoaringBitmap();
    private final FSDataOutputStream _output;
    private final int _blockSize;
    private final List<String> _sourceFiles;

    private long _prevKey = Long.MIN_VALUE;

    private Writer(FileSystem fileSystem, Path path, int blockSize, List<String> sourceFiles) throws IOException {
      _output = fileSystem.create(path);
      _blockSize = blockSize;
      _sourceFiles = sourceFiles;
    }

    public void appendEmpty(int longKey) throws IOException {
      int key = checkKey(longKey);
      _emptyBlocks.add(key);
      _prevKey = longKey;
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
      writeStringList(_output, _sourceFiles);
      _output.writeInt(MAGIC_STR.length);
      _output.write(MAGIC_STR);
      _output.writeLong(pos);
      _output.close();
    }

    public long getLen() throws IOException {
      return _output.getPos();
    }

  }

  public abstract static class Reader implements Iterable<BlockFileEntry>, Closeable {

    protected final RoaringBitmap _blocks = new RoaringBitmap();
    protected final RoaringBitmap _emptyBlocks = new RoaringBitmap();
    protected final FSDataInputStream _inputStream;
    protected final int _blockSize;
    protected final Path _path;
    protected final List<String> _sourceFiles;

    protected Reader(FileSystem fileSystem, Path path) throws IOException {
      _inputStream = fileSystem.open(path);
      _path = path;
      FileStatus fileStatus = fileSystem.getFileStatus(path);
      long len = fileStatus.getLen();
      _inputStream.seek(len - 8);
      long metaDataPosition = _inputStream.readLong();
      _inputStream.seek(metaDataPosition);
      _blocks.deserialize(_inputStream);
      _emptyBlocks.deserialize(_inputStream);
      _blockSize = _inputStream.readInt();
      _sourceFiles = readStringList(_inputStream);
      // @TODO read and validate the magic string
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

    public RoaringBitmap getBlocks() {
      return _blocks;
    }

    public RoaringBitmap getEmptyBlocks() {
      return _emptyBlocks;
    }

    public boolean hasEmptyBlock(int blockId) {
      return _emptyBlocks.contains(blockId);
    }

    public boolean hasBlock(int blockId) {
      return _blocks.contains(blockId);
    }

    public Path getPath() {
      return _path;
    }

    public int getBlockSize() {
      return _blockSize;
    }

    protected void readBlock(int key, BytesWritable value) throws IOException {
      int storageBlockPosition = _blocks.rank(key) - 1;
      long position = storageBlockPosition * (long) _blockSize;
      _inputStream.read(position, value.getBytes(), 0, _blockSize);
    }

    protected void setAllZeros(BytesWritable value) {
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

    protected Iterator<BlockFileEntry> newIndexIterator(PeekableIterator<Integer> emptyIterator,
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

          if (b == null) {
            int id = emptyIterator.next();
            return newBlockFileEntry(id, true);
          }

          if (e == null) {
            int id = blocksIterator.next();
            return newBlockFileEntry(id, false);
          }

          if (e.compareTo(b) < 0) {
            int id = emptyIterator.next();
            return newBlockFileEntry(id, true);
          } else {
            int id = blocksIterator.next();
            return newBlockFileEntry(id, false);
          }
        }

      };
    }

    protected Long toLong(Integer i) {
      return (long) ((int) i);
    }

    protected BlockFileEntry newBlockFileEntry(int id, boolean empty) {
      return new BlockFileEntry() {

        @Override
        public long getBlockId() {
          return id;
        }

        @Override
        public boolean isEmpty() {
          return empty;
        }

        @Override
        public void readData(BytesWritable value) throws IOException {
          value.setCapacity(_blockSize);
          value.setSize(_blockSize);
          readBlock(id, value);
        }
      };
    }

    public List<String> getSourceBlockFiles() {
      return _sourceFiles;
    }

  }

  public static class RandomAccessReader extends Reader {
    public RandomAccessReader(FileSystem fileSystem, Path path) throws IOException {
      super(fileSystem, path);
      try {
        _inputStream.setReadahead(0l);
      } catch (UnsupportedOperationException e) {
        LOGGER.debug("Can not set readahead for path {}", path);
      }
    }
  }

  public static class StreamReader extends Reader {

    private final long _maxSkip = 128 * 1024 * 1024;
    private final Object _lock = new Object();

    public StreamReader(FileSystem fileSystem, Path path) throws IOException {
      super(fileSystem, path);
    }

    protected void readBlock(int key, BytesWritable value) throws IOException {
      synchronized (_lock) {
        int storageBlockPosition = _blocks.rank(key) - 1;
        long position = storageBlockPosition * (long) _blockSize;
        long pos = _inputStream.getPos();
        long skip = position - pos;
        if (skip < 0 || skip > _maxSkip) {
          _inputStream.seek(position);
        } else {
          _inputStream.skip(skip);
        }
        _inputStream.read(value.getBytes(), 0, _blockSize);
      }
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

    void readData(BytesWritable value) throws IOException;

  }

  public static List<String> readStringList(DataInput input) throws IOException {
    int length = input.readInt();
    if (length < 0) {
      return null;
    } else if (length == 0) {
      return ImmutableList.of();
    } else {
      List<String> list = new ArrayList<>();
      for (int i = 0; i < length; i++) {
        list.add(readString(input));
      }
      return list;
    }
  }

  public static void writeStringList(DataOutput output, List<String> list) throws IOException {
    if (list == null) {
      output.writeInt(-1);
      return;
    }
    if (list.isEmpty()) {
      output.writeInt(0);
      return;
    }
    output.writeInt(list.size());
    for (String s : list) {
      writeString(output, s);
    }
  }

  public static String readString(DataInput input) throws IOException {
    int length = input.readInt();
    if (length < 0) {
      return null;
    }
    byte[] buf = new byte[length];
    input.readFully(buf);
    return new String(buf, UTF_8);
  }

  public static void writeString(DataOutput output, String s) throws IOException {
    if (s == null) {
      output.writeInt(-1);
      return;
    }
    byte[] bs = s.getBytes(UTF_8);
    output.writeInt(bs.length);
    output.write(bs);
  }
}
