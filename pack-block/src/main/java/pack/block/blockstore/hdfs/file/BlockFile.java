package pack.block.blockstore.hdfs.file;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockFile.class);
  private static final String HDFS_BLOCK_FILE_V1 = "hdfs_block_file_v1";
  private static final String UTF_8 = "UTF-8";
  private static final byte[] MAGIC_STR;

  static {
    try {
      MAGIC_STR = HDFS_BLOCK_FILE_V1.getBytes(UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Writer create(FileSystem fileSystem, Path path, int blockSize, List<String> sourceFileList)
      throws IOException {
    return new WriterOrdered(fileSystem, path, blockSize, sourceFileList, null);
  }

  public static Writer create(FileSystem fileSystem, Path path, int blockSize, List<String> sourceFileList,
      CommitFile commitFile) throws IOException {
    return new WriterOrdered(fileSystem, path, blockSize, sourceFileList, commitFile);
  }

  public static Writer create(FileSystem fileSystem, Path path, int blockSize, CommitFile commitFile)
      throws IOException {
    return new WriterOrdered(fileSystem, path, blockSize, ImmutableList.of(), commitFile);
  }

  public static Writer create(FileSystem fileSystem, Path path, int blockSize) throws IOException {
    return new WriterOrdered(fileSystem, path, blockSize, ImmutableList.of(), null);
  }

  public static Reader open(FileSystem fileSystem, Path path) throws IOException {
    return new RandomAccessReaderOrdered(fileSystem, path);
  }

  public static Reader openForStreaming(FileSystem fileSystem, Path path) throws IOException {
    return new StreamReaderOrdered(fileSystem, path);
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
      reader.orDataBlocks(result);
      reader.orEmptyBlocks(result);
    });
    return result;
  }

  public static abstract class Writer implements Closeable {

    public abstract boolean canAppend(long longKey) throws IOException;

    public abstract void appendEmpty(long longKey) throws IOException;

    public abstract void append(long longKey, BytesWritable value) throws IOException;

    public abstract long getLen() throws IOException;

  }

  public static class WriterUnordered extends Writer {

    private final FSDataOutputStream _output;
    private final AtomicReference<WriterOrdered> _currentWriter = new AtomicReference<>();
    private int _blockSize;

    private WriterUnordered(FSDataOutputStream output, int blockSize) throws IOException {
      _output = output;
      _blockSize = blockSize;
    }

    private WriterUnordered(FileSystem fileSystem, Path path, int blockSize) throws IOException {
      this(fileSystem.create(path), blockSize);
    }

    @Override
    public boolean canAppend(long longKey) throws IOException {
      return true;
    }

    @Override
    public void appendEmpty(long longKey) throws IOException {
      WriterOrdered writer = getWriter();
      if (!writer.canAppend(longKey)) {
        writer = newWriter();
      }
      writer.appendEmpty(longKey);
    }

    @Override
    public void append(long longKey, BytesWritable value) throws IOException {
      WriterOrdered writer = getWriter();
      if (!writer.canAppend(longKey)) {
        writer = newWriter();
      }
    }

    @Override
    public long getLen() throws IOException {
      return _output.getPos();
    }

    @Override
    public void close() throws IOException {
      WriterOrdered writer = getWriter();
      if (writer != null) {
        writer.writeFooter();
      }
      _output.close();
    }

    private WriterOrdered newWriter() throws IOException {
      WriterOrdered writer = _currentWriter.get();
      if (writer != null) {
        writer.writeFooter();
      }
      writer = new WriterOrdered(_output, _blockSize, ImmutableList.of(), null);
      _currentWriter.set(writer);
      return writer;
    }

    private WriterOrdered getWriter() {
      return _currentWriter.get();
    }

  }

  public static class WriterOrdered extends Writer {

    private final RoaringBitmap _blocks = new RoaringBitmap();
    private final RoaringBitmap _emptyBlocks = new RoaringBitmap();
    private final FSDataOutputStream _output;
    private final int _blockSize;
    private final List<String> _sourceFiles;
    private final CommitFile _commitFile;

    private long _prevKey = Long.MIN_VALUE;

    private WriterOrdered(FSDataOutputStream output, int blockSize, List<String> sourceFiles, CommitFile commitFile)
        throws IOException {
      _output = output;
      _blockSize = blockSize;
      _sourceFiles = sourceFiles;
      _commitFile = commitFile;
    }

    private WriterOrdered(FileSystem fileSystem, Path path, int blockSize, List<String> sourceFiles,
        CommitFile commitFile) throws IOException {
      this(fileSystem.create(path), blockSize, sourceFiles, commitFile);
    }

    @Override
    public boolean canAppend(long longKey) throws IOException {
      if (longKey <= _prevKey) {
        return false;
      }
      getIntKey(longKey);
      return true;
    }

    @Override
    public void appendEmpty(long longKey) throws IOException {
      int key = checkKey(longKey);
      _emptyBlocks.add(key);
      _prevKey = longKey;
    }

    @Override
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

    @Override
    public long getLen() throws IOException {
      return _output.getPos();
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
      writeFooter();
      _output.close();
      if (_commitFile != null) {
        _commitFile.commit();
      }
    }

    public void writeFooter() throws IOException {
      long pos = _output.getPos();
      _blocks.serialize(_output);
      _emptyBlocks.serialize(_output);
      _output.writeInt(_blockSize);
      writeStringList(_output, _sourceFiles);
      _output.writeInt(MAGIC_STR.length);
      _output.write(MAGIC_STR);
      _output.writeLong(pos);
    }

  }

  public abstract static class Reader implements Iterable<BlockFileEntry>, Closeable {

    public abstract boolean read(List<ReadRequest> requests) throws IOException;

    public abstract boolean read(long longKey, BytesWritable value) throws IOException;

    // public abstract RoaringBitmap getBlocks();

    public abstract void orDataBlocks(RoaringBitmap bitmap);

    // public abstract RoaringBitmap getEmptyBlocks();

    public abstract void orEmptyBlocks(RoaringBitmap bitmap);

    public abstract boolean hasEmptyBlock(int blockId);

    public abstract boolean hasBlock(int blockId);

    public abstract Path getPath();

    public abstract int getBlockSize();

    public abstract List<String> getSourceBlockFiles();

  }

  public abstract static class ReaderOrdered extends Reader {

    protected final RoaringBitmap _blocks = new RoaringBitmap();
    protected final RoaringBitmap _emptyBlocks = new RoaringBitmap();
    protected final FSDataInputStream _inputStream;
    protected final int _blockSize;
    protected final Path _path;
    protected final List<String> _sourceFiles;
    protected final long _first;
    protected final long _last;

    protected ReaderOrdered(FileSystem fileSystem, Path path) throws IOException {
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
      _first = getFirst();
      _last = getLast();
      // @TODO read and validate the magic string
    }

    private int getFirst() {
      if (_blocks.isEmpty() && _emptyBlocks.isEmpty()) {
        return Integer.MAX_VALUE;
      } else if (_blocks.isEmpty()) {
        return _emptyBlocks.first();
      } else if (_emptyBlocks.isEmpty()) {
        return _blocks.first();
      } else {
        return Math.min(_blocks.first(), _emptyBlocks.first());
      }
    }

    private int getLast() {
      if (_blocks.isEmpty() && _emptyBlocks.isEmpty()) {
        return Integer.MIN_VALUE;
      } else if (_blocks.isEmpty()) {
        return _emptyBlocks.last();
      } else if (_emptyBlocks.isEmpty()) {
        return _blocks.last();
      } else {
        return Math.max(_blocks.last(), _emptyBlocks.last());
      }
    }

    private final int _maxParallelBlocksToPull = 32;

    /**
     * Return true if more read requests remain.
     * 
     * @param requests
     * @return
     * @throws IOException
     */
    @Override
    public boolean read(List<ReadRequest> requests) throws IOException {
      Collections.sort(requests, (o1, o2) -> Long.compare(o1.getBlockId(), o2.getBlockId()));
      // @TODO, combine requests???
      boolean moreRequestsNeeded = false;
      byte[] hdfsBuffer = new byte[_maxParallelBlocksToPull * _blockSize];
      int startBlockIndex = -1;
      int prevBlockIndex = -1;
      ReadRequest[] requestBatch = new ReadRequest[_maxParallelBlocksToPull];
      for (ReadRequest readRequest : requests) {
        if (!readRequest.isCompleted()) {
          long blockId = readRequest.getBlockId();
          int key = getIntKey(blockId);
          if (_emptyBlocks.contains(key)) {
            readRequest.handleEmptyResult();
          } else if (_blocks.contains(key)) {
            int storageBlockIndex = _blocks.rank(key) - 1;
            if (startBlockIndex < 0) {
              startBlockIndex = storageBlockIndex;
            }

            if (storageBlockIndex - startBlockIndex >= _maxParallelBlocksToPull) {
              // can't add request to current batch, would be too large
              readFromHdfs(hdfsBuffer, startBlockIndex, prevBlockIndex, requestBatch);
              Arrays.fill(requestBatch, null);
              startBlockIndex = storageBlockIndex;
            }

            prevBlockIndex = storageBlockIndex;
            requestBatch[storageBlockIndex - startBlockIndex] = readRequest;
          } else {
            moreRequestsNeeded = true;
          }
        }
      }

      if (startBlockIndex >= 0) {
        readFromHdfs(hdfsBuffer, startBlockIndex, prevBlockIndex, requestBatch);
      }

      return moreRequestsNeeded;
    }

    private void readFromHdfs(byte[] hdfsBuffer, int startBlockIndex, int prevBlockIndex, ReadRequest[] requestBatch)
        throws IOException {
      int numberOfContiguousBlock = (prevBlockIndex - startBlockIndex) + 1;
      long position = startBlockIndex * (long) _blockSize;
      _inputStream.read(position, hdfsBuffer, 0, numberOfContiguousBlock * _blockSize);
      // handle requests...
      for (int i = 0; i < requestBatch.length; i++) {
        ReadRequest batchRequest = requestBatch[i];
        if (batchRequest != null) {
          int offset = i * _blockSize;
          batchRequest.handleResult(hdfsBuffer, offset);
        }
      }
    }

    @Override
    public boolean read(long longKey, BytesWritable value) throws IOException {
      if (longKey < _first) {
        return false;
      } else if (longKey > _last) {
        return false;
      }
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

    public void orDataBlocks(RoaringBitmap bitmap) {
      bitmap.or(_blocks);
    }

    public void orEmptyBlocks(RoaringBitmap bitmap) {
      bitmap.or(_emptyBlocks);
    }

    @Override
    public boolean hasEmptyBlock(int blockId) {
      return _emptyBlocks.contains(blockId);
    }

    @Override
    public boolean hasBlock(int blockId) {
      return _blocks.contains(blockId);
    }

    @Override
    public Path getPath() {
      return _path;
    }

    @Override
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

    @Override
    public List<String> getSourceBlockFiles() {
      return _sourceFiles;
    }

  }

  public static class RandomAccessReaderOrdered extends ReaderOrdered {
    public RandomAccessReaderOrdered(FileSystem fileSystem, Path path) throws IOException {
      super(fileSystem, path);
      try {
        _inputStream.setReadahead(0l);
      } catch (UnsupportedOperationException e) {
        LOGGER.debug("Can not set readahead for path {}", path);
      }
    }
  }

  public static class StreamReaderOrdered extends ReaderOrdered {

    private final long _maxSkip = 128 * 1024 * 1024;
    private final Object _lock = new Object();

    public StreamReaderOrdered(FileSystem fileSystem, Path path) throws IOException {
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
