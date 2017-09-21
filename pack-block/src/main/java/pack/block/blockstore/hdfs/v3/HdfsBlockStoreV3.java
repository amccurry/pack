package pack.block.blockstore.hdfs.v3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;
import pack.block.blockstore.hdfs.file.BlockFile.Writer;
import pack.block.blockstore.hdfs.file.ReadRequest;
import pack.block.server.fs.LinuxFileSystem;
import pack.block.util.Utils;

public class HdfsBlockStoreV3 implements HdfsBlockStore {

  private static final String TMP = ".tmp";

  private final static Logger LOGGER = LoggerFactory.getLogger(HdfsBlockStoreV3.class);

  private static final String READ_METER = "readMeter";
  private static final String WRITE_METER = "writeMeter";
  private static final String READ_TIMER = "readTimer";
  private static final String WRITE_TIMER = "writeTimer";

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

  public HdfsBlockStoreV3(MetricRegistry registry, FileSystem fileSystem, Path path) throws IOException {
    this(registry, fileSystem, path, HdfsBlockStoreConfig.DEFAULT_CONFIG);
  }

  public HdfsBlockStoreV3(MetricRegistry registry, FileSystem fileSystem, Path path, HdfsBlockStoreConfig config)
      throws IOException {

    _registry = registry;

    _writeTimer = _registry.timer(WRITE_TIMER);
    _readTimer = _registry.timer(READ_TIMER);

    _writeMeter = _registry.meter(WRITE_METER);
    _readMeter = _registry.meter(READ_METER);

    _fileSystem = fileSystem;
    _path = qualify(path);
    _metaData = HdfsBlockStoreAdmin.readMetaData(_fileSystem, _path);

    _fileSystemBlockSize = _metaData.getFileSystemBlockSize();

    _length = _metaData.getLength();
    _blockPath = qualify(new Path(_path, HdfsBlockStoreConfig.BLOCK));
    _fileSystem.mkdirs(_blockPath);
    cleanupBlocks();
    RemovalListener<Path, BlockFile.Reader> listener = notification -> IOUtils.closeQuietly(notification.getValue());
    _readerCache = CacheBuilder.newBuilder()
                               .removalListener(listener)
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

  private void cleanupBlocks() throws IOException {
    FileStatus[] listStatus = _fileSystem.listStatus(_blockPath);
    for (FileStatus fileStatus : listStatus) {
      if (shouldCleanupFile(fileStatus.getPath())) {
        _fileSystem.delete(fileStatus.getPath(), false);
      } else if (shouldTryToRecover(fileStatus.getPath())) {
        recoverBlock(fileStatus.getPath());
      }
    }
  }

  private void recoverBlock(Path path) {
    LOGGER.info("Recover block {}", path);

  }

  private boolean shouldTryToRecover(Path path) {
    return false;
  }

  private boolean shouldCleanupFile(Path path) {
    return path.getName()
               .endsWith(TMP);
  }

  public int getFileSystemBlockSize() {
    return _fileSystemBlockSize;
  }

  @Override
  public void close() throws IOException {
    fsync();
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

  private final AtomicReference<ActiveWriter> _currentActive = new AtomicReference<>();

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
        ActiveWriter writer = createWriterIfNeeded();
        writer.append(blockId, byteBuffer);
        _writeMeter.mark(len);
        return len;
      }
    } finally {
      _fileWriteLock.unlock();
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
      checkActiveWriterCache(requests);
      commitActiveWriterIfNeeded(requests);
      readBlocks(requests);
      _readMeter.mark(len);
      return len;
    } finally {
      // Nothing
    }
  }

  private void checkActiveWriterCache(List<ReadRequest> requests) {
    ActiveWriter activeWriter = _currentActive.get();
    if (activeWriter == null) {
      return;
    }
    activeWriter.checkCache(requests);
  }

  @Override
  public void fsync() throws IOException {
    LOGGER.debug("fsync");
    commitActiveWriter();
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
      ActiveWriter writer = createWriterIfNeeded();
      for (long blockId = startingBlockId; blockId < endingBlockId; blockId++) {
        writer.appendEmpty(blockId);
      }
    } finally {
      _fileWriteLock.unlock();
    }
  }

  private ActiveWriter createWriterIfNeeded() throws IOException {
    _fileWriteLock.lock();
    try {
      LOGGER.debug("createWriterIfNeeded");
      ActiveWriter activeWriter = _currentActive.get();
      if (activeWriter != null) {
        return activeWriter;
      }
      Path path = getNewTempFile();
      LOGGER.debug("created Writer {}", path);
      Writer writer = BlockFile.create(false, _fileSystem, path, _fileSystemBlockSize, () -> commitFile(path));
      RoaringBitmap index = new RoaringBitmap();
      activeWriter = new ActiveWriter(writer, index);
      _currentActive.set(activeWriter);
      return activeWriter;
    } finally {
      _fileWriteLock.unlock();
    }
  }

  private void commitActiveWriterIfNeeded(List<ReadRequest> requests) throws IOException {
    _fileWriteLock.lock();
    LOGGER.debug("commitActiveWriterIfNeeded");
    try {
      ActiveWriter activeWriter = _currentActive.get();
      if (activeWriter == null) {
        return;
      }
      for (ReadRequest readRequest : requests) {
        if (!readRequest.isCompleted() && activeWriter.contains(Utils.getIntKey(readRequest.getBlockId()))) {
          commitActiveWriter();
          return;
        }
      }
    } finally {
      _fileWriteLock.unlock();
    }
  }

  private void commitActiveWriter() throws IOException {
    _fileWriteLock.lock();
    try {
      LOGGER.debug("commitActiveWriter");
      ActiveWriter activeWriter = _currentActive.get();
      if (activeWriter == null) {
        return;
      }
      activeWriter.close();
      _currentActive.set(null);
    } finally {
      _fileWriteLock.unlock();
    }
  }

  private void commitFile(Path path) throws IOException {
    _blockFileWriteLock.lock();
    try {
      Path blockPath = getNewBlockFilePath();
      LOGGER.debug("commitFile {} to {}", path, blockPath);
      if (_fileSystem.rename(path, blockPath)) {
        getReader(blockPath);// open file ahead of time
        Builder<Path> builder = ImmutableList.builder();
        builder.add(blockPath);
        List<Path> list = _blockFiles.get();
        if (list != null) {
          builder.addAll(list);
        }
        _blockFiles.set(builder.build());
      } else {
        throw new IOException("Could not commit tmp block " + path + " to " + blockPath);
      }
    } finally {
      _blockFileWriteLock.unlock();
    }
  }

  private Path getNewTempFile() {
    String uuid = UUID.randomUUID()
                      .toString()
        + TMP;
    Path path = qualify(new Path(_blockPath, uuid));
    return path;
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
    return qualify(new Path(_path, "block." + Long.toString(hdfsBlock)));
  }

  private Path getNewBlockFilePath() {
    return qualify(BlockFile.getNewPathFile(_blockPath));
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

  private Path qualify(Path path) {
    return path.makeQualified(_fileSystem.getUri(), _fileSystem.getWorkingDirectory());
  }

  private List<Path> getBlockFilePathListFromStorage() throws FileNotFoundException, IOException {
    List<Path> pathList = new ArrayList<>();
    FileStatus[] listStatus = _fileSystem.listStatus(_blockPath, (PathFilter) p -> BlockFile.isOrderedBlock(p));
    Arrays.sort(listStatus, Collections.reverseOrder());

    for (FileStatus fileStatus : listStatus) {
      pathList.add(qualify(fileStatus.getPath()));
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
      removeBlockFile(qualify(new Path(_blockPath, name)));
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

}
