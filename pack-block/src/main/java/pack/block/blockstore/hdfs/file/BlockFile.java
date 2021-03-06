package pack.block.blockstore.hdfs.file;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.io.Closer;

import pack.block.blockstore.hdfs.ReadRequestHandler;
import pack.block.blockstore.hdfs.util.HdfsUtils;
import pack.block.blockstore.hdfs.util.PeekableIterator;
import pack.block.util.Utils;

public class BlockFile {

  private static final String BLOCKLNK = ".blocklnk";

  private static final int DEFAULT_MERGE_READ_BATCH_SIZE = 32;
  private static final String BLOCK = ".block";
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

  public static boolean createLinkDir(FileSystem fileSystem, Path srcDir, Path destDir) throws IOException {
    if (!fileSystem.exists(srcDir) || !fileSystem.isDirectory(srcDir)) {
      return false;
    }
    fileSystem.mkdirs(destDir);
    {
      FileStatus[] listStatus = fileSystem.listStatus(destDir);
      if (listStatus.length != 0) {
        return false;
      }
    }
    FileStatus[] listStatus = fileSystem.listStatus(srcDir, (PathFilter) path -> isOrderedBlock(path));
    for (FileStatus fileStatus : listStatus) {
      createLinkPath(fileSystem, fileStatus.getPath(), destDir);
    }
    return true;
  }

  public static void createLinkPath(FileSystem fileSystem, Path srcFile, Path destDir) throws IOException {
    String name = srcFile.getName();
    int indexOf = name.indexOf('.');
    String prefix = name.substring(0, indexOf);
    Path destFile = new Path(destDir, prefix + BLOCKLNK);
    String path = srcFile.toUri()
                         .getPath();
    try (PrintWriter writer = new PrintWriter(fileSystem.create(destFile))) {
      writer.println(path);
    }
  }

  public static Writer create(boolean ordered, FileSystem fileSystem, Path path, int blockSize,
      List<String> sourceFileList, CommitFile commitFile) throws IOException {
    if (ordered) {
      return new WriterOrdered(fileSystem, path, blockSize, sourceFileList, commitFile);
    } else {
      if (!sourceFileList.isEmpty()) {
        throw new IOException("source files should be empty");
      }
      return new WriterMultiOrdered(fileSystem, path, blockSize, commitFile);
    }
  }

  public static Writer create(boolean ordered, FileSystem fileSystem, Path path, int blockSize,
      List<String> sourceFileList) throws IOException {
    return create(ordered, fileSystem, path, blockSize, sourceFileList, null);
  }

  public static Writer create(boolean ordered, FileSystem fileSystem, Path path, int blockSize, CommitFile commitFile)
      throws IOException {
    return create(ordered, fileSystem, path, blockSize, ImmutableList.of(), commitFile);
  }

  public static Writer create(boolean ordered, FileSystem fileSystem, Path path, int blockSize) throws IOException {
    return create(ordered, fileSystem, path, blockSize, ImmutableList.of(), null);
  }

  public static WriterOrdered createOrdered(FileSystem fileSystem, Path path, int blockSize,
      List<String> sourceFileList, CommitFile commitFile) throws IOException {
    return new WriterOrdered(fileSystem, path, blockSize, sourceFileList, commitFile);
  }

  public static WriterOrdered createOrdered(FileSystem fileSystem, Path path, int blockSize,
      List<String> sourceFileList) throws IOException {
    return createOrdered(fileSystem, path, blockSize, sourceFileList, null);
  }

  public static WriterOrdered createOrdered(FileSystem fileSystem, Path path, int blockSize, CommitFile commitFile)
      throws IOException {
    return createOrdered(fileSystem, path, blockSize, ImmutableList.of(), commitFile);
  }

  public static WriterOrdered createOrdered(FileSystem fileSystem, Path path, int blockSize) throws IOException {
    return createOrdered(fileSystem, path, blockSize, ImmutableList.of(), null);
  }

  public static Reader open(FileSystem fileSystem, Path path) throws IOException {
    Path resolved = resolvePath(fileSystem, path);
    FSDataInputStream inputStream = fileSystem.open(resolved);
    long length = getLength(fileSystem, resolved);
    if (isMultiOrderedBlock(inputStream, length)) {
      return new ReaderMultiOrdered(inputStream, path, resolved, length);
    }
    return new RandomAccessReaderOrdered(inputStream, path, resolved, length);
  }

  public static Path resolvePath(FileSystem fileSystem, Path path) throws IOException {
    if (path.getName()
            .endsWith(BLOCKLNK)) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)))) {
        String line = reader.readLine();
        return fileSystem.makeQualified(new Path(line.trim()));
      }
    }
    return path;
  }

  public static ReaderMultiOrdered openMultiOrdered(FileSystem fileSystem, Path path, long length) throws IOException {
    Path resolved = resolvePath(fileSystem, path);
    FSDataInputStream inputStream = getInputStream(fileSystem, resolved, length);
    return new ReaderMultiOrdered(inputStream, path, resolved, length);
  }

  public static Reader openForStreaming(FileSystem fileSystem, Path path) throws IOException {
    Path resolved = resolvePath(fileSystem, path);
    FSDataInputStream inputStream = fileSystem.open(resolved);
    long length = getLength(fileSystem, resolved);
    if (isMultiOrderedBlock(inputStream, length)) {
      return new ReaderMultiOrdered(inputStream, path, resolved, length);
    }
    return new StreamReaderOrdered(inputStream, path, resolved, length);
  }

  public static boolean isMultiOrderedBlock(FSDataInputStream input, long length) throws IOException {
    return !isSingleOrderedBlock(input, length);
  }

  public static boolean isSingleOrderedBlock(FSDataInputStream input, long length) throws IOException {
    input.seek(length - 16);
    if (input.readLong() == 0) {
      return true;
    }
    return false;
  }

  public static boolean isOrderedBlock(Path path) {
    return path.getName()
               .endsWith(BLOCK)
        || path.getName()
               .endsWith(BLOCKLNK);
  }

  public static void merge(List<Reader> readers, WriterOrdered writer) throws IOException {
    merge(readers, writer, null);
  }

  public static void merge(List<Reader> readers, WriterOrdered writer, RoaringBitmap blocksToIgnore)
      throws IOException {
    merge(readers, writer, blocksToIgnore, DEFAULT_MERGE_READ_BATCH_SIZE);
  }

  public static void merge(List<Reader> readers, WriterOrdered writer, RoaringBitmap blocksToIgnore, int batchSize)
      throws IOException {
    MergePlan mergePlan = new MergePlan(readers, blocksToIgnore, batchSize);
    mergePlan.runMerge(writer);
  }

  public static class MergePlan {

    private final List<Reader> _readers;
    private final RoaringBitmap _blocksToIgnore;
    private final List<RoaringBitmap> _dataBlocksToFetch;
    private final RoaringBitmap _emptyBlocks;
    private final int _blockSize;
    private final int _requestsPerBlock;

    public MergePlan(List<Reader> readers, RoaringBitmap blocksToIgnore, int requestsPerBlock) {
      _requestsPerBlock = requestsPerBlock;
      _readers = orderReaders(readers);
      _blockSize = _readers.get(0)
                           .getBlockSize();
      _blocksToIgnore = blocksToIgnore;
      _dataBlocksToFetch = calculateDataBlocksPerReader();
      _emptyBlocks = calculateFinalEmptyBlocks();

    }

    public void runMerge(WriterOrdered writer) throws IOException {
      List<PeekableIterator<Integer>> blockItList = new ArrayList<>();
      for (RoaringBitmap dataBlock : _dataBlocksToFetch) {
        blockItList.add(PeekableIterator.wrap(dataBlock.iterator()));
      }

      List<Queue<ReadRequest>> requestsList = new ArrayList<>();
      for (int i = 0; i < _readers.size(); i++) {
        requestsList.add(new LinkedList<>());
      }

      long last = 0;
      final long delay = TimeUnit.SECONDS.toMillis(5);
      long dataBlockTotal = getDataBlockTotal();
      long dataBlockWriteCount = 0;
      long countWriteCount = 0;

      while (!isFinished(blockItList, requestsList)) {
        long now = System.currentTimeMillis();
        if (last + delay < now) {
          LOGGER.info("merge {}% complete, count {} total {} dataBlocks {}",
              ((long) (((double) countWriteCount / (double) dataBlockTotal) * 1000) / 10.0), countWriteCount,
              dataBlockTotal, dataBlockWriteCount);
          last = System.currentTimeMillis();
          dataBlockWriteCount = 0;
        }

        populateRequests(_requestsPerBlock, blockItList, requestsList);
        ReadRequest readRequest = findNextReadRequest(requestsList);
        ByteBuffer byteBuffer = readRequest.getByteBuffer();
        byteBuffer.flip();
        writer.append(readRequest.getBlockId(), Utils.toBw(byteBuffer));
        dataBlockWriteCount++;
        countWriteCount++;
      }

      RoaringBitmap dataBlocksInternal = writer.getDataBlocksInternal();
      RoaringBitmap clone = dataBlocksInternal.clone();
      clone.and(_emptyBlocks);
      if (clone.getCardinality() != 0) {
        throw new IOException("Cardinality check failed for merged output.");
      }
      RoaringBitmap emptyBlocksInternal = writer.getEmptyBlocksInternal();
      emptyBlocksInternal.or(_emptyBlocks);
      LOGGER.info("merge {}% complete, count {} total {} dataBlocks {}",
          ((long) (((double) countWriteCount / (double) dataBlockTotal) * 1000) / 10.0), countWriteCount,
          dataBlockTotal, dataBlockWriteCount);
    }

    private long getDataBlockTotal() {
      long total = 0;
      for (RoaringBitmap bitmap : _dataBlocksToFetch) {
        total += bitmap.getLongCardinality();
      }
      return total;
    }

    private ReadRequest findNextReadRequest(List<Queue<ReadRequest>> requestsList) throws IOException {
      ReadRequest readRequest = null;
      int index = -1;
      for (int i = 0; i < requestsList.size(); i++) {
        Queue<ReadRequest> requests = requestsList.get(i);
        if (requests.isEmpty()) {
          continue;
        }
        if (readRequest == null) {
          readRequest = requests.peek();
          index = i;
        } else {
          ReadRequest rr = requests.peek();
          int compareTo = rr.compareTo(readRequest);
          if (compareTo < 0) {
            readRequest = requests.peek();
            index = i;
          } else if (compareTo == 0) {
            throw new IOException("This should not happen!!! ReadRequests have same id.");
          }
        }
      }
      if (index < 0) {
        throw new IOException("No ReadRequest found.");
      }
      Queue<ReadRequest> queue = requestsList.get(index);
      if (!readRequest.isCompleted()) {
        Reader reader = _readers.get(index);
        reader.read(queue);
      }
      ReadRequest request = queue.remove();
      LOGGER.debug("Found ReadRequest {} from block {}", readRequest, _readers.get(index));
      return request;
    }

    private boolean isFinished(List<PeekableIterator<Integer>> blockItList, List<Queue<ReadRequest>> requestsList) {
      for (PeekableIterator<Integer> blockIt : blockItList) {
        if (blockIt.hasNext()) {
          return false;
        }
      }
      for (Queue<ReadRequest> requests : requestsList) {
        if (!requests.isEmpty()) {
          return false;
        }
      }
      return true;
    }

    private void populateRequests(int requestsPerBlock, List<PeekableIterator<Integer>> blockItList,
        List<Queue<ReadRequest>> requestsList) {
      for (int i = 0; i < blockItList.size(); i++) {
        Queue<ReadRequest> requests = requestsList.get(i);
        PeekableIterator<Integer> blockIt = blockItList.get(i);
        while (requests.size() < requestsPerBlock && blockIt.hasNext()) {
          int blockId = blockIt.next();
          ByteBuffer dest = ByteBuffer.allocate(_blockSize);
          requests.add(new ReadRequest(blockId, 0, dest));
        }
      }
    }

    private RoaringBitmap calculateFinalEmptyBlocks() {
      RoaringBitmap emptyBlocks = new RoaringBitmap();
      List<Reader> prevReaders = new ArrayList<>();
      for (int i = 0; i < _readers.size(); i++) {
        Reader reader = _readers.get(i);
        calculateEmptyBlocks(reader, prevReaders, emptyBlocks);
        prevReaders.add(reader);
      }
      if (_blocksToIgnore != null) {
        emptyBlocks.andNot(_blocksToIgnore);
      }
      return emptyBlocks;
    }

    private void calculateEmptyBlocks(Reader reader, List<Reader> prevReaders, RoaringBitmap finalEmptyBlocks) {
      RoaringBitmap emptyBlocks = new RoaringBitmap();
      reader.orEmptyBlocks(emptyBlocks);
      for (Reader r : prevReaders) {
        RoaringBitmap blocks = new RoaringBitmap();
        r.orDataBlocks(blocks);
        emptyBlocks.andNot(blocks);
      }
      finalEmptyBlocks.or(emptyBlocks);
    }

    private List<RoaringBitmap> calculateDataBlocksPerReader() {
      List<RoaringBitmap> dataBlocksToFetch = new ArrayList<>();
      List<Reader> prevReaders = new ArrayList<>();
      for (int i = 0; i < _readers.size(); i++) {
        Reader reader = _readers.get(i);
        dataBlocksToFetch.add(calculateDataBlocks(reader, prevReaders));
        prevReaders.add(reader);
      }
      return dataBlocksToFetch;
    }

    private RoaringBitmap calculateDataBlocks(Reader reader, List<Reader> prevReaders) {
      RoaringBitmap dataBlocks = new RoaringBitmap();
      reader.orDataBlocks(dataBlocks);
      for (Reader r : prevReaders) {
        RoaringBitmap blocks = new RoaringBitmap();
        r.orDataBlocks(blocks);
        r.orEmptyBlocks(blocks);
        dataBlocks.andNot(blocks);
      }
      if (_blocksToIgnore != null) {
        dataBlocks.andNot(_blocksToIgnore);
      }
      return dataBlocks;
    }
  }

  public static abstract class Writer implements Closeable {

    public abstract boolean canAppend(long longKey) throws IOException;

    public abstract void appendEmpty(long longKey) throws IOException;

    public abstract void append(long longKey, BytesWritable value) throws IOException;

    public abstract long getLen() throws IOException;

  }

  public static class WriterMultiOrdered extends Writer {

    private final FSDataOutputStream _output;
    private final AtomicReference<WriterOrdered> _currentWriter = new AtomicReference<>();
    private final int _blockSize;
    private final CommitFile _commitFile;

    private WriterMultiOrdered(FSDataOutputStream output, int blockSize, CommitFile commitFile) throws IOException {
      _output = output;
      _blockSize = blockSize;
      _commitFile = commitFile;
    }

    private WriterMultiOrdered(FileSystem fileSystem, Path path, int blockSize, CommitFile commitFile)
        throws IOException {
      this(fileSystem.create(path), blockSize, commitFile);
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
      writer.append(longKey, value);
    }

    @Override
    public long getLen() throws IOException {
      return _output.getPos();
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
      WriterOrdered writer = _currentWriter.getAndSet(null);
      if (writer != null) {
        writer.writeFooter();
      }
    }

    public long sync() throws IOException {
      _output.hflush();
      return _output.getPos();
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

    private WriterOrdered getWriter() throws IOException {
      WriterOrdered writerOrdered = _currentWriter.get();
      if (writerOrdered == null) {
        return newWriter();
      }
      return writerOrdered;
    }

  }

  public static class WriterOrdered extends Writer {

    private final RoaringBitmap _blocks = new RoaringBitmap();
    private final RoaringBitmap _emptyBlocks = new RoaringBitmap();
    private final FSDataOutputStream _output;
    private final int _blockSize;
    private final List<String> _sourceFiles;
    private final CommitFile _commitFile;
    private final long _startingPosition;

    private long _prevKey = Long.MIN_VALUE;

    private WriterOrdered(FSDataOutputStream output, int blockSize, List<String> sourceFiles, CommitFile commitFile)
        throws IOException {
      _output = output;
      _startingPosition = _output.getPos();
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
      Utils.getIntKey(longKey);
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
      return Utils.getIntKey(key);
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
      _output.writeLong(_startingPosition);
      _output.writeLong(pos);
    }

    public RoaringBitmap getEmptyBlocksInternal() {
      return _emptyBlocks;
    }

    public RoaringBitmap getDataBlocksInternal() {
      return _blocks;
    }
  }

  public abstract static class Reader implements ReadRequestHandler, Iterable<BlockFileEntry>, Closeable {

    @Override
    public boolean readBlocks(List<ReadRequest> requests) throws IOException {
      return read(requests);
    }

    public abstract boolean read(Collection<ReadRequest> requests) throws IOException;

    public abstract boolean read(long longKey, BytesWritable value) throws IOException;

    public abstract void orDataBlocks(RoaringBitmap bitmap);

    public abstract void orEmptyBlocks(RoaringBitmap bitmap);

    public abstract boolean hasEmptyBlock(int blockId);

    public abstract boolean hasBlock(int blockId);

    public abstract Path getLogicalPath();

    public abstract Path getStoragePath();

    public abstract int getBlockSize();

    public abstract List<String> getSourceBlockFiles();

    public abstract int getHeapSize();

    @Override
    public String toString() {
      Path path = getLogicalPath();
      return "Reader " + path.getParent()
                             .getName()
          + "/" + path.getName();
    }

    public abstract void andNotEmptyBlocks(RoaringBitmap bitmap);

  }

  public static class ReaderMultiOrdered extends Reader {

    private final FSDataInputStream _inputStream;
    private final List<RandomAccessReaderOrdered> _orderedReaders;
    private final int _blockSize;
    private final Path _logicalPath;
    private final Path _storagePath;
    private final long _layer;

    protected ReaderMultiOrdered(FSDataInputStream inputStream, Path logicalPath, Path storagePath, long length)
        throws IOException {
      this(inputStream, logicalPath, storagePath, length, ImmutableList.of());
    }

    protected ReaderMultiOrdered(FSDataInputStream inputStream, Path logicalPath, Path storagePath, long length,
        List<RandomAccessReaderOrdered> existingReaders) throws IOException {
      _inputStream = inputStream;
      _logicalPath = logicalPath;
      _storagePath = storagePath;
      _orderedReaders = openOrderedReaders(length, existingReaders);
      _blockSize = _orderedReaders.get(0)
                                  .getBlockSize();
      _layer = BlockFile.getLayer(logicalPath);
    }

    private List<RandomAccessReaderOrdered> openOrderedReaders(long length,
        List<RandomAccessReaderOrdered> existingReaders) throws IOException {
      long endingPosition = length;
      Builder<RandomAccessReaderOrdered> builder = ImmutableList.builder();
      while (endingPosition >= 0) {
        RandomAccessReaderOrdered readerOrdered = openOrderedReader(existingReaders, endingPosition);
        builder.add(readerOrdered);
        long startingPosition = readerOrdered.getStartingPosition();
        if (startingPosition == 0) {
          return builder.build();
        }
        endingPosition = startingPosition;
      }
      // this should never be reached.
      throw new IOException("Malformed file " + _logicalPath + " " + _storagePath);
    }

    private RandomAccessReaderOrdered openOrderedReader(List<RandomAccessReaderOrdered> existingReaders,
        long endingPosition) throws IOException {
      for (RandomAccessReaderOrdered readerOrdered : existingReaders) {
        if (readerOrdered._endingPosition == endingPosition) {
          return new RandomAccessReaderOrdered(readerOrdered, _inputStream);
        }
      }
      return new RandomAccessReaderOrdered(_inputStream, _logicalPath, _storagePath, endingPosition);
    }

    public ReaderMultiOrdered reopen(FileSystem fileSystem, long newLength) throws IOException {
      FSDataInputStream newInputStream = getInputStream(fileSystem, _storagePath, newLength);
      return new ReaderMultiOrdered(newInputStream, _logicalPath, _storagePath, newLength, _orderedReaders);
    }

    @Override
    public Iterator<BlockFileEntry> iterator() {
      return null;
    }

    @Override
    public void close() throws IOException {
      _orderedReaders.forEach(t -> Utils.close(LOGGER, t));
    }

    /**
     * Returns boolean true if more requests needed, false if all requests are
     * fulfilled.
     */
    @Override
    public boolean read(Collection<ReadRequest> requests) throws IOException {
      for (ReaderOrdered readerOrdered : _orderedReaders) {
        if (!readerOrdered.read(requests)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean read(long longKey, BytesWritable value) throws IOException {
      for (ReaderOrdered readerOrdered : _orderedReaders) {
        if (readerOrdered.read(longKey, value)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void orDataBlocks(RoaringBitmap bitmap) {
      _orderedReaders.forEach(t -> t.orDataBlocks(bitmap));
    }

    @Override
    public void orEmptyBlocks(RoaringBitmap bitmap) {
      _orderedReaders.forEach(t -> t.orEmptyBlocks(bitmap));
    }

    @Override
    public void andNotEmptyBlocks(RoaringBitmap bitmap) {
      _orderedReaders.forEach(t -> t.andNotEmptyBlocks(bitmap));
    }

    @Override
    public boolean hasEmptyBlock(int blockId) {
      for (ReaderOrdered readerOrdered : _orderedReaders) {
        if (readerOrdered.hasBlock(blockId)) {
          return false;
        } else if (readerOrdered.hasEmptyBlock(blockId)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean hasBlock(int blockId) {
      for (ReaderOrdered readerOrdered : _orderedReaders) {
        if (readerOrdered.hasBlock(blockId)) {
          return true;
        } else if (readerOrdered.hasEmptyBlock(blockId)) {
          return false;
        }
      }
      return false;
    }

    @Override
    public int getBlockSize() {
      return _blockSize;
    }

    @Override
    public List<String> getSourceBlockFiles() {
      Builder<String> builder = ImmutableList.builder();
      for (ReaderOrdered readerOrdered : _orderedReaders) {
        builder.addAll(readerOrdered.getSourceBlockFiles());
      }
      return builder.build();
    }

    @Override
    public int getHeapSize() {
      int total = 0;
      for (RandomAccessReaderOrdered rand : _orderedReaders) {
        total += rand.getHeapSize();
      }
      return total;
    }

    @Override
    public Path getLogicalPath() {
      return _logicalPath;
    }

    @Override
    public Path getStoragePath() {
      return _storagePath;
    }

    @Override
    public long getLayer() {
      return _layer;
    }

  }

  public abstract static class ReaderOrdered extends Reader {

    protected final ImmutableRoaringBitmap _blocks;
    protected final ImmutableRoaringBitmap _emptyBlocks;
    protected final FSDataInputStream _inputStream;
    protected final int _blockSize;
    protected final List<String> _sourceFiles;
    protected final long _first;
    protected final long _last;
    protected final long _startingPosition;
    protected final long _endingPosition;
    protected final Closer _closer = Closer.create();
    protected final Path _logicalPath;
    protected final Path _storagePath;
    protected final long _layer;

    protected ReaderOrdered(ReaderOrdered reader, FSDataInputStream inputStream) throws IOException {
      _blocks = reader._blocks;
      _emptyBlocks = reader._emptyBlocks;
      _inputStream = _closer.register(inputStream);
      _blockSize = reader._blockSize;
      _logicalPath = reader._logicalPath;
      _storagePath = reader._storagePath;
      _sourceFiles = reader._sourceFiles;
      _first = reader._first;
      _last = reader._last;
      _startingPosition = reader._startingPosition;
      _endingPosition = reader._endingPosition;
      _layer = BlockFile.getLayer(_logicalPath);
    }

    protected ReaderOrdered(FSDataInputStream inputStream, Path logicalPath, Path storagePath, long endingPosition)
        throws IOException {
      _endingPosition = endingPosition;
      _logicalPath = logicalPath;
      _storagePath = storagePath;
      _inputStream = _closer.register(inputStream);
      _inputStream.seek(endingPosition - 16);
      _startingPosition = _inputStream.readLong();
      long metaDataPosition = _inputStream.readLong();
      _inputStream.seek(metaDataPosition);

      _blocks = ImmutableRoaringBitmapManager.load(logicalPath.getName() + "-data", _inputStream);
      _emptyBlocks = ImmutableRoaringBitmapManager.load(logicalPath.getName() + "-empty", _inputStream);

      _blockSize = _inputStream.readInt();
      _sourceFiles = readStringList(_inputStream);
      _first = getFirst();
      _last = getLast();
      _layer = BlockFile.getLayer(_logicalPath);
      // @TODO read and validate the magic string
    }

    @Override
    public long getLayer() {
      return _layer;
    }

    @Override
    public int getHeapSize() {
      int total = _blocks.getSizeInBytes();
      total += _emptyBlocks.getSizeInBytes();
      return total;
    }

    public long getStartingPosition() {
      return _startingPosition;
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
    public boolean read(Collection<ReadRequest> requestsCollection) throws IOException {
      List<ReadRequest> requests = new ArrayList<>(requestsCollection);
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
          int key = Utils.getIntKey(blockId);
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
      _inputStream.read(position + _startingPosition, hdfsBuffer, 0, numberOfContiguousBlock * _blockSize);
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
      int key = Utils.getIntKey(longKey);
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

    @Override
    public void orDataBlocks(RoaringBitmap bitmap) {
      bitmap.or(_blocks.toRoaringBitmap());
    }

    @Override
    public void orEmptyBlocks(RoaringBitmap bitmap) {
      bitmap.or(_emptyBlocks.toRoaringBitmap());
    }

    @Override
    public void andNotEmptyBlocks(RoaringBitmap bitmap) {
      bitmap.andNot(_emptyBlocks.toRoaringBitmap());
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
    public int getBlockSize() {
      return _blockSize;
    }

    @Override
    public Path getLogicalPath() {
      return _logicalPath;
    }

    @Override
    public Path getStoragePath() {
      return _storagePath;
    }

    protected void readBlock(int key, BytesWritable value) throws IOException {
      int storageBlockPosition = _blocks.rank(key) - 1;
      long position = storageBlockPosition * (long) _blockSize;
      _inputStream.read(position + _startingPosition, value.getBytes(), 0, _blockSize);
    }

    protected void setAllZeros(BytesWritable value) {
      int length = value.getLength();
      Arrays.fill(value.getBytes(), 0, length, (byte) 0);
    }

    @Override
    public void close() throws IOException {
      _closer.close();
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

    public RandomAccessReaderOrdered(FSDataInputStream inputStream, Path logicalPath, Path storagePath, long endOfBlock)
        throws IOException {
      super(inputStream, logicalPath, storagePath, endOfBlock);
      try {
        _inputStream.setReadahead(0l);
      } catch (UnsupportedOperationException e) {
        LOGGER.debug("Can not set readahead for path {} {}", logicalPath, storagePath);
      }
    }

    public RandomAccessReaderOrdered(ReaderOrdered reader, FSDataInputStream inputStream) throws IOException {
      super(reader, inputStream);
    }

  }

  public static class StreamReaderOrdered extends ReaderOrdered {

    private final long _maxSkip = 128 * 1024 * 1024;
    private final Object _lock = new Object();

    public StreamReaderOrdered(FSDataInputStream inputStream, Path logicalPath, Path storagePath, long endingPosition)
        throws IOException {
      super(inputStream, logicalPath, storagePath, endingPosition);
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

  private static long getLength(FileSystem fileSystem, Path path) throws IOException {
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    return fileStatus.getLen();
  }

  public static Path getNewPathFile(Path dir) {
    return new Path(dir, System.currentTimeMillis() + BlockFile.BLOCK);
  }

  public static FSDataInputStream getInputStream(FileSystem fileSystem, Path path, long newLength) throws IOException {
    while (true) {
      FSDataInputStream inputStream = fileSystem.open(path);
      long fileLength = HdfsUtils.getFileLength(fileSystem, path, inputStream);
      if (newLength <= fileLength) {
        return inputStream;
      }
      Utils.close(LOGGER, inputStream);
      try {
        Thread.sleep(400);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  public static Comparator<Long> ORDERED_LAYER_COMPARATOR = new Comparator<Long>() {
    @Override
    public int compare(Long o1, Long o2) {
      return Long.compare(o2, o1);
    }
  };

  public static Comparator<Path> ORDERED_PATH_COMPARATOR = new Comparator<Path>() {
    @Override
    public int compare(Path o1, Path o2) {
      return ORDERED_LAYER_COMPARATOR.compare(getLayer(o1), getLayer(o2));
    }
  };

  public static Comparator<FileStatus> ORDERED_FILESTATUS_COMPARATOR = new Comparator<FileStatus>() {
    @Override
    public int compare(FileStatus o1, FileStatus o2) {
      return ORDERED_PATH_COMPARATOR.compare(o1.getPath(), o2.getPath());
    }
  };

  public static Comparator<Reader> ORDERED_READER_COMPARATOR = new Comparator<Reader>() {
    @Override
    public int compare(Reader o1, Reader o2) {
      return ORDERED_PATH_COMPARATOR.compare(o1.getLogicalPath(), o2.getLogicalPath());
    }
  };

  public static Comparator<ReadRequestHandler> ORDERED_READ_REQUEST_HANDLER_COMPARATOR = new Comparator<ReadRequestHandler>() {
    @Override
    public int compare(ReadRequestHandler o1, ReadRequestHandler o2) {
      return ORDERED_LAYER_COMPARATOR.compare(o1.getLayer(), o2.getLayer());
    }
  };

  public static List<Reader> orderReaders(List<Reader> readers) {
    List<Reader> ordered = new ArrayList<>(readers);
    Collections.sort(ordered, ORDERED_READER_COMPARATOR);
    return ordered;
  }

  public static long getLayer(String name) {
    int indexOf = name.indexOf('.');
    return Long.parseLong(name.substring(0, indexOf));
  }

  public static long getLen(FileSystem fileSystem, Path path) throws IOException {
    Path resolvePath = resolvePath(fileSystem, path);
    FileStatus fileStatus = fileSystem.getFileStatus(resolvePath);
    return fileStatus.getLen();
  }

  public static long getLayer(File file) {
    return getLayer(file.getName());
  }

  public static long getLayer(Path path) {
    return getLayer(path.getName());
  }
}
