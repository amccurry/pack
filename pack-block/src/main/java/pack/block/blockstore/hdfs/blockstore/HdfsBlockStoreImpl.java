package pack.block.blockstore.hdfs.blockstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;
import pack.block.blockstore.hdfs.file.ReadRequest;
import pack.block.blockstore.hdfs.file.WalKeyWritable;
import pack.block.server.fs.LinuxFileSystem;
import pack.block.util.Utils;

public class HdfsBlockStoreImpl implements HdfsBlockStore {

  private static final String WAL_TMP = ".waltmp";
  private static final String WAL = ".wal";

  private final static Logger LOGGER = LoggerFactory.getLogger(HdfsBlockStoreImpl.class);

  private static final String READ_METER = "readMeter";
  private static final String WRITE_METER = "writeMeter";
  private static final String READ_TIMER = "readTimer";
  private static final String WRITE_TIMER = "writeTimer";
  private static final BytesWritable EMPTY_BLOCK = new BytesWritable();

  private final FileSystem _fileSystem;
  private final Path _path;
  private final HdfsMetaData _metaData;
  private final long _length;
  private final int _fileSystemBlockSize;
  private final Path _blockPath;
  private final Cache<Path, BlockFile.Reader> _readerCache;
  private final AtomicReference<List<Path>> _blockFiles = new AtomicReference<>();
  private final Timer _blockFileTimer;
  private final MetricRegistry _registry;
  private final com.codahale.metrics.Timer _writeTimer;
  private final com.codahale.metrics.Timer _readTimer;
  private final Meter _writeMeter;
  private final Meter _readMeter;
  private final Lock _fileWriteLock = new ReentrantReadWriteLock().writeLock();
  private final ReentrantReadWriteLock _blockFileLock = new ReentrantReadWriteLock();
  private final Lock _blockFileWriteLock = _blockFileLock.writeLock();
  private final Lock _blockFileReadLock = _blockFileLock.readLock();
  private final AtomicReference<WriterContext> _writer = new AtomicReference<>();
  private final long _maxWalSize;
  private final AtomicReference<List<Path>> _localCacheList = new AtomicReference<List<Path>>(ImmutableList.of());
  private final Cache<Path, LocalWalCache> _localCache;
  private final File _cacheDir;
  private final AtomicLong _genCounter;
  private final WalFileFactory _walFactory;

  public HdfsBlockStoreImpl(MetricRegistry registry, File cacheDir, FileSystem fileSystem, Path path) throws IOException {
    this(registry, cacheDir, fileSystem, path, HdfsBlockStoreConfig.DEFAULT_CONFIG);
  }

  public HdfsBlockStoreImpl(MetricRegistry registry, File cacheDir, FileSystem fileSystem, Path path,
      HdfsBlockStoreConfig config) throws IOException {

    _registry = registry;

    _writeTimer = _registry.timer(WRITE_TIMER);
    _readTimer = _registry.timer(READ_TIMER);

    _writeMeter = _registry.meter(WRITE_METER);
    _readMeter = _registry.meter(READ_METER);

    _fileSystem = fileSystem;
    _path = Utils.qualify(fileSystem, path);
    _metaData = HdfsBlockStoreAdmin.readMetaData(_fileSystem, _path);

    _walFactory = WalFileFactory.create(_fileSystem, _metaData);

    _cacheDir = cacheDir;
    Utils.rmr(_cacheDir);
    _cacheDir.mkdirs();
    _localCache = CacheBuilder.newBuilder()
                              .removalListener(getRemovalListener())
                              .build();

    _maxWalSize = _metaData.getMaxWalFileSize();

    _fileSystemBlockSize = _metaData.getFileSystemBlockSize();

    _length = _metaData.getLength();
    _blockPath = Utils.qualify(fileSystem, new Path(_path, HdfsBlockStoreConfig.BLOCK));
    _fileSystem.mkdirs(_blockPath);
    _genCounter = new AtomicLong(readGenCounter());

    loadWalFiles();
    RemovalListener<Path, BlockFile.Reader> readerListener = notification -> IOUtils.closeQuietly(
        notification.getValue());
    _readerCache = CacheBuilder.newBuilder()
                               .removalListener(readerListener)
                               .build();

    List<Path> pathList = getBlockFilePathListFromStorage();
    _blockFiles.set(ImmutableList.copyOf(pathList));
    // create background thread that removes orphaned block files and checks for
    // new block files that have been merged externally
    _blockFileTimer = new Timer(HdfsBlockStoreConfig.BLOCK + "|" + _blockPath.toUri()
                                                                             .getPath(),
        true);
    long period = config.getBlockFileUnit()
                        .toMillis(config.getBlockFilePeriod());
    _blockFileTimer.schedule(getBlockFileTask(), period, period);
  }

  private RemovalListener<Path, LocalWalCache> getRemovalListener() {
    return notification -> {
      synchronized (_localCacheList) {
        List<Path> newList = new ArrayList<>(_localCacheList.get());
        newList.remove(notification.getKey());
        _localCacheList.set(ImmutableList.copyOf(newList));
      }
      LocalWalCache localWalCache = notification.getValue();
      LOGGER.info("Closing and removing local wal cache {}", localWalCache);
      Utils.close(LOGGER, localWalCache);
    };
  }

  private long readGenCounter() throws IOException {
    FileStatus[] listStatus = _fileSystem.listStatus(_blockPath, (PathFilter) path -> {
      String name = path.getName();
      return BlockFile.isOrderedBlock(path) || name.endsWith(WAL) || name.endsWith(WAL_TMP);
    });
    if (listStatus.length == 0) {
      return 0;
    }
    Arrays.sort(listStatus, BlockFile.ORDERED_FILESTATUS_COMPARATOR);
    return readGen(listStatus[0]);
  }

  private long readGen(FileStatus fileStatus) throws IOException {
    return readGen(fileStatus.getPath());
  }

  private long readGen(Path path) throws IOException {
    return readGen(path.getName());
  }

  private long readGen(String name) throws IOException {
    int indexOf = name.indexOf('.');
    if (indexOf < 0) {
      throw new IOException("Malformed file " + name);
    }
    return Long.parseLong(name.substring(0, indexOf));
  }

  private void loadWalFiles() throws IOException {
    FileStatus[] listStatus = _fileSystem.listStatus(_blockPath);
    List<Path> walPathList = new ArrayList<>();
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      if (shouldTryToRecover(path)) {
        recoverBlock(path);
      }
    }
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      if (shouldTryToPullWal(path)) {
        LocalWalCache localWalCache = getLocalWalCache(path);
        LocalWalCache.applyWal(_walFactory, path, localWalCache);
        walPathList.add(path);
      }
    }
    Collections.sort(walPathList, BlockFile.ORDERED_PATH_COMPARATOR);
    synchronized (_localCacheList) {
      _localCacheList.set(ImmutableList.copyOf(walPathList));
    }
  }

  private LocalWalCache getLocalWalCache(Path path) throws IOException {
    LocalWalCache localWalCache;
    try {
      localWalCache = _localCache.get(path, getValueLoader(path));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(cause);
    }
    return localWalCache;
  }

  private Callable<? extends LocalWalCache> getValueLoader(Path path) {
    return () -> {
      LOGGER.info("Loading cache for path {}", path);
      synchronized (_localCacheList) {
        List<Path> newList = new ArrayList<>(_localCacheList.get());
        newList.add(path);
        Collections.sort(newList, BlockFile.ORDERED_PATH_COMPARATOR);
        _localCacheList.set(ImmutableList.copyOf(newList));
      }
      return new LocalWalCache(new File(_cacheDir, path.getName()), _length, _metaData.getFileSystemBlockSize());
    };
  }

  private LocalWalCache getCurrentLocalContext(WriterContext writer) throws IOException {
    return getLocalWalCache(writer.path);
  }

  private Path newDataGenerationFile(String ext) {
    long gen = _genCounter.incrementAndGet();
    return Utils.qualify(_fileSystem, new Path(_blockPath, Long.toString(gen) + ext));
  }

  private Path newExtPath(Path path, String ext) throws IOException {
    String name = path.getName();
    int lastIndexOf = name.lastIndexOf('.');
    if (lastIndexOf < 0) {
      throw new IOException("Path " + path + " has no ext");
    }
    return new Path(path.getParent(), name.substring(0, lastIndexOf) + ext);
  }

  private boolean shouldTryToPullWal(Path path) {
    return path.getName()
               .endsWith(WAL);
  }

  private void recoverBlock(Path src) throws IOException {
    LOGGER.info("Recover block {}", src);
    Path dst = newDataGenerationFile(WAL_TMP);
    _walFactory.recover(src, dst);
    if (_fileSystem.rename(dst, newExtPath(dst, WAL))) {
      LOGGER.info("Recovery of block {} complete", src);
      _fileSystem.delete(src, false);
    }
  }

  private boolean shouldTryToRecover(Path path) {
    return path.getName()
               .endsWith(WAL_TMP);
  }

  public int getFileSystemBlockSize() {
    return _fileSystemBlockSize;
  }

  @Override
  public void close() throws IOException {
    closeWriter();
    _blockFileTimer.cancel();
    _blockFileTimer.purge();
    _readerCache.invalidateAll();
  }

  @Override
  public long getLength() {
    return _length;
  }

  @Override
  public String getName() {
    return _path.getName();
  }

  @Override
  public long lastModified() {
    return System.currentTimeMillis();
  }

  @Override
  public int write(long position, byte[] buffer, int offset, int len) throws IOException {
    _fileWriteLock.lock();
    try {
      int blockSize = _fileSystemBlockSize;
      try (Context context = _writeTimer.time()) {
        int blockOffset = getBlockOffset(position);
        long blockId = getBlockId(position);
        LOGGER.debug("write blockId {} blockOffset {} position {}", blockId, blockOffset, position);
        ByteBuffer byteBuffer;
        if (blockOffset == 0 && len == blockSize) {
          // no reads needed
          byteBuffer = ByteBuffer.wrap(buffer, offset, blockSize);
        } else {
          long blockAlignedPosition = blockId * blockSize;
          byte[] buf = new byte[blockSize];
          read(blockAlignedPosition, buf, 0, blockSize);

          len = Math.min(blockSize - blockOffset, len);
          System.arraycopy(buffer, offset, buf, blockOffset, len);
          byteBuffer = ByteBuffer.wrap(buf, 0, blockSize);
        }
        WriterContext writer = getWriter();
        writer.append(blockId, getValue(byteBuffer));
        LocalWalCache local = getCurrentLocalContext(writer);
        local.write(blockId, byteBuffer);
        _writeMeter.mark(len);
        return len;
      }
    } finally {
      _fileWriteLock.unlock();
    }
  }

  private WriterContext getWriter() throws IOException {
    synchronized (_writer) {
      WriterContext context = _writer.get();
      if (context == null) {
        Path tmpPath = newDataGenerationFile(WAL_TMP);
        context = new WriterContext(_walFactory.create(tmpPath), tmpPath);
        _writer.set(context);
      }
      return context;
    }
  }

  private void closeWriter() throws IOException {
    synchronized (_writer) {
      WriterContext context = _writer.getAndSet(null);
      if (context != null) {
        context.writer.close();
        _fileSystem.rename(context.path, newExtPath(context.path, WAL));
      }
    }
  }

  private void flushWriter() throws IOException {
    synchronized (_writer) {
      WriterContext writer = _writer.get();
      if (writer == null) {
        return;
      }
      if (writer.getLength() >= _maxWalSize) {
        LOGGER.info("Wal file length too large, closing file");
        closeWriter();
      } else {
        writer.flush();
      }
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int len) throws IOException {
    int blockSize = _fileSystemBlockSize;
    try (Context context = _readTimer.time()) {
      long blockId = getBlockId(position);
      LOGGER.debug("read blockId {} len {} position {}", blockId, len, position);
      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, len);
      List<ReadRequest> requests = createRequests(position, byteBuffer, blockSize);
      if (readBlocksFromLocalWalCaches(requests)) {
        readBlocks(requests);
      }
      _readMeter.mark(len);
      return len;
    } finally {
      // Nothing
    }
  }

  private boolean readBlocksFromLocalWalCaches(List<ReadRequest> requests) throws IOException {
    List<Path> walPathList = _localCacheList.get();
    for (Path walPath : walPathList) {
      LocalWalCache localWalCache = getLocalWalCache(walPath);
      if (!localWalCache.readBlocks(requests)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void fsync() throws IOException {
    LOGGER.debug("fsync");
    // Utils.time(LOGGER, "fsync", () -> {
    // flushWriter();
    // return null;
    // });
    flushWriter();
  }

  @Override
  public void delete(long position, long length) throws IOException {
    LOGGER.debug("delete position {} length {}", position, length);
    long startingBlockId = getBlockId(position);
    if (getBlockOffset(position) != 0) {
      // move to next full block
      startingBlockId++;
    }

    long endingPosition = position + length;
    long endingBlockId = getBlockId(endingPosition);

    // Delete all block from start (inclusive) to end (exclusive)
    _fileWriteLock.lock();
    try {
      WriterContext writer = getWriter();
      LocalWalCache localContext = getCurrentLocalContext(writer);
      writer.delete(startingBlockId, endingBlockId);
      localContext.delete(startingBlockId, endingBlockId);
    } finally {
      _fileWriteLock.unlock();
    }
  }

  private void readBlocks(List<ReadRequest> requests) throws IOException {
    _blockFileReadLock.lock();
    try {
      List<Path> list = _blockFiles.get();
      if (list != null) {
        for (Path path : list) {
          Reader reader = getReader(path);
          if (!reader.read(requests)) {
            return;
          }
        }
      }
    } finally {
      _blockFileReadLock.unlock();
    }
  }

  private List<ReadRequest> createRequests(long position, ByteBuffer byteBuffer, int blockSize) {
    int remaining = byteBuffer.remaining();
    int bufferPosition = 0;
    List<ReadRequest> result = new ArrayList<>();
    while (remaining > 0) {
      int blockOffset = getBlockOffset(position);
      long blockId = getBlockId(position);
      int len = Math.min(blockSize - blockOffset, remaining);

      byteBuffer.position(bufferPosition);
      byteBuffer.limit(bufferPosition + len);

      ByteBuffer slice = byteBuffer.slice();
      result.add(new ReadRequest(blockId, blockOffset, slice));

      position += len;
      bufferPosition += len;
      remaining -= len;
    }
    return result;
  }

  protected Path getHdfsBlockPath(long hdfsBlock) {
    return Utils.qualify(_fileSystem, new Path(_path, "block." + Long.toString(hdfsBlock)));
  }

  private Reader getReader(Path path) throws IOException {
    try {
      LOGGER.debug("getReader {}", path);
      return _readerCache.get(path, () -> BlockFile.open(_fileSystem, path));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  private long getBlockId(long position) {
    return position / _fileSystemBlockSize;
  }

  @Override
  public HdfsMetaData getMetaData() {
    return _metaData;
  }

  @Override
  public LinuxFileSystem getLinuxFileSystem() {
    return _metaData.getFileSystemType()
                    .getLinuxFileSystem();
  }

  private List<Path> getBlockFilePathListFromStorage() throws FileNotFoundException, IOException {
    List<Path> pathList = new ArrayList<>();
    FileStatus[] listStatus = _fileSystem.listStatus(_blockPath, (PathFilter) p -> BlockFile.isOrderedBlock(p));
    Arrays.sort(listStatus, BlockFile.ORDERED_FILESTATUS_COMPARATOR);

    for (FileStatus fileStatus : listStatus) {
      pathList.add(Utils.qualify(_fileSystem, fileStatus.getPath()));
    }
    return pathList;
  }

  private TimerTask getBlockFileTask() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          processBlockFiles();
        } catch (Throwable t) {
          LOGGER.error("Unknown error trying to clean old block files.", t);
        }
      }
    };
  }

  public void processBlockFiles() throws IOException {
    loadAnyMissingBlockFiles();
    dropOldBlockFiles();
  }

  private void dropOldBlockFiles() throws IOException {
    List<Path> blockFiles = _blockFiles.get();
    for (Path path : blockFiles) {
      if (!_fileSystem.exists(path)) {
        LOGGER.info("Path no longer exists, due to old block files being removed {}", path);
        continue;
      }
      Reader reader = getReader(path);
      List<String> sourceBlockFiles = reader.getSourceBlockFiles();
      if (sourceBlockFiles != null) {
        removeBlockFiles(sourceBlockFiles);
      }
    }
  }

  private void removeBlockFiles(List<String> sourceBlockFiles) throws IOException {
    for (String name : sourceBlockFiles) {
      invalidateLocalCache(name);
      Path path = Utils.qualify(_fileSystem, new Path(_blockPath, name));
      if (path.getName()
              .endsWith(HdfsBlockStoreConfig.BLOCK)
          && _fileSystem.exists(path)) {
        Reader reader = getReader(path);
        removeBlockFiles(reader.getSourceBlockFiles());
      }
      removeBlockFile(path);
    }
  }

  private void invalidateLocalCache(String name) throws IOException {
    if (name.endsWith(WAL) || name.endsWith(WAL_TMP)) {
      Path path = Utils.qualify(_fileSystem, new Path(_blockPath, name));
      _localCache.invalidate(newExtPath(path, WAL));
      _localCache.invalidate(newExtPath(path, WAL_TMP));
    }
  }

  private void removeBlockFile(Path path) throws IOException {
    _blockFileWriteLock.lock();
    try {
      if (!_fileSystem.exists(path)) {
        return;
      }
      LOGGER.info("Removing old block file {}", path);
      List<Path> list = new ArrayList<>(_blockFiles.get());
      list.remove(path);
      _blockFiles.set(ImmutableList.copyOf(list));
      _readerCache.invalidate(path);
      _fileSystem.delete(path, true);
    } finally {
      _blockFileWriteLock.unlock();
    }
  }

  /**
   * Open stored files that are missing from the cache, these are likely from an
   * external compaction.
   * 
   * @return newly opened block files.
   * @throws IOException
   */
  private void loadAnyMissingBlockFiles() throws IOException {
    List<Path> storageList;
    List<Path> cacheList;

    _blockFileWriteLock.lock();
    try {
      cacheList = new ArrayList<>(_blockFiles.get());
      storageList = getBlockFilePathListFromStorage();
      if (LOGGER.isDebugEnabled()) {
        storageList.forEach(path -> LOGGER.debug("Storage path {}", path));
        cacheList.forEach(path -> LOGGER.debug("Cache path {}", path));
      }

      if (storageList.equals(cacheList)) {
        LOGGER.debug("No missing block files to load.");
        return;
      }
      if (!storageList.containsAll(cacheList)) {
        cacheList.removeAll(storageList);
        LOGGER.error("Cache list contains references to files that no longer exist {}", cacheList);
        throw new IOException("Missing files error.");
      }
      _blockFiles.set(ImmutableList.copyOf(storageList));
    } finally {
      _blockFileWriteLock.unlock();
    }

    List<Path> newFiles = new ArrayList<>(storageList);
    newFiles.removeAll(cacheList);
    LOGGER.info("New files found.");
    for (Path path : storageList) {
      LOGGER.info("Loading {}.", path);
      getReader(path);
    }
  }

  private int getBlockOffset(long position) {
    return (int) (position % _fileSystemBlockSize);
  }

  private BytesWritable getValue(ByteBuffer byteBuffer) {
    return Utils.toBw(byteBuffer);
  }

  private static class WriterContext {
    final WalFile.Writer writer;
    final Path path;

    WriterContext(WalFile.Writer writer, Path path) {
      this.writer = writer;
      this.path = path;
    }

    void delete(long startingBlockId, long endingBlockId) throws IOException {
      WalKeyWritable key = new WalKeyWritable(startingBlockId, endingBlockId);
      writer.append(key, EMPTY_BLOCK);
    }

    void append(long blockId, BytesWritable value) throws IOException {
      WalKeyWritable key = new WalKeyWritable(blockId);
      writer.append(key, value);
    }

    long getLength() throws IOException {
      return writer.getLength();
    }

    void flush() throws IOException {
      writer.flush();
    }
  }
}