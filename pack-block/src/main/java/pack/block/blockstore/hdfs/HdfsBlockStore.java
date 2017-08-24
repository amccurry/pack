package pack.block.blockstore.hdfs;

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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.block.blockstore.BlockStore;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;
import pack.block.blockstore.hdfs.file.BlockFile.Writer;
import pack.block.blockstore.hdfs.kvs.ExternalWriter;
import pack.block.blockstore.hdfs.kvs.HdfsKeyValueStore;
import pack.block.server.fs.LinuxFileSystem;

public class HdfsBlockStore implements BlockStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsBlockStore.class);

  public static final String BLOCK = "block";
  public static final String KVS = "kvs";

  private final FileSystem _fileSystem;
  private final Path _path;
  private final HdfsMetaData _metaData;
  private final long _length;
  private final Timer _hdfsKeyValueTimer;
  private final HdfsKeyValueStore _hdfsKeyValueStore;
  private final int _fileSystemBlockSize;
  private final Path _blockPath;
  private final long _maxMemory;
  private final Cache<Path, BlockFile.Reader> _readerCache;
  private final byte[] _emptyBlock;
  private final AtomicReference<List<Path>> _blockFiles = new AtomicReference<>();
  private final Timer _blockFileTimer;
  private final int _maxMemoryEntries;

  public HdfsBlockStore(FileSystem fileSystem, Path path) throws IOException {
    this(fileSystem, path, HdfsBlockStoreConfig.DEFAULT_CONFIG);
  }

  public HdfsBlockStore(FileSystem fileSystem, Path path, HdfsBlockStoreConfig config) throws IOException {
    _fileSystem = fileSystem;
    _path = qualify(path);
    _metaData = HdfsBlockStoreAdmin.readMetaData(_fileSystem, _path);

    _fileSystemBlockSize = _metaData.getFileSystemBlockSize();
    _maxMemoryEntries = config.getMaxMemoryEntries();
    _emptyBlock = new byte[_fileSystemBlockSize];
    _maxMemory = config.getMaxMemoryForCache();

    _length = _metaData.getLength();
    Path kvPath = qualify(new Path(_path, KVS));
    _blockPath = qualify(new Path(_path, BLOCK));
    _fileSystem.mkdirs(_blockPath);
    _hdfsKeyValueTimer = new Timer(KVS + "|" + kvPath, true);
    _hdfsKeyValueStore = new HdfsKeyValueStore(false, _hdfsKeyValueTimer, fileSystem.getConf(), kvPath);
    RemovalListener<Path, BlockFile.Reader> listener = notification -> IOUtils.closeQuietly(notification.getValue());
    _readerCache = CacheBuilder.newBuilder()
                               .removalListener(listener)
                               .build();
    List<Path> pathList = getBlockFilePathListFromStorage();
    _blockFiles.set(ImmutableList.copyOf(pathList));
    // create background thread that removes orphaned block files and checks for
    // new block files that have been merged externally
    _blockFileTimer = new Timer(BLOCK + "|" + _blockPath.toUri()
                                                        .getPath(),
        true);
    long period = config.getBlockFileUnit()
                        .toMillis(config.getBlockFilePeriod());
    _blockFileTimer.scheduleAtFixedRate(getBlockFileTask(), period, period);
  }

  private Path qualify(Path path) {
    return path.makeQualified(_fileSystem.getUri(), _fileSystem.getWorkingDirectory());
  }

  private List<Path> getBlockFilePathListFromStorage() throws FileNotFoundException, IOException {
    List<Path> pathList = new ArrayList<>();
    FileStatus[] listStatus = _fileSystem.listStatus(_blockPath, (PathFilter) p -> p.getName()
                                                                                    .endsWith("." + BLOCK));
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
    List<Path> newBlockFiles = loadAnyMissingBlockFiles();
    dropOldBlockFiles(newBlockFiles);
  }

  private void dropOldBlockFiles(List<Path> newBlockFiles) throws IOException {
    for (Path path : newBlockFiles) {
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
    LOGGER.info("Removing old block file {}", path);
    synchronized (_blockFiles) {
      List<Path> list = new ArrayList<>(_blockFiles.get());
      list.remove(path);
      _blockFiles.set(ImmutableList.copyOf(list));
    }
    _readerCache.invalidate(path);
    _fileSystem.delete(path, true);
  }

  /**
   * Open stored files that are missing from the cache, these are likely from an
   * external compaction.
   * 
   * @return newly opened block files.
   * @throws IOException
   */
  private List<Path> loadAnyMissingBlockFiles() throws IOException {
    List<Path> storageList;
    List<Path> cacheList;
    synchronized (_blockFiles) {
      cacheList = new ArrayList<>(_blockFiles.get());
      storageList = getBlockFilePathListFromStorage();
      storageList.forEach(path -> LOGGER.debug("Storage path {}", path));
      cacheList.forEach(path -> LOGGER.debug("Cache path {}", path));

      if (storageList.equals(cacheList)) {
        LOGGER.debug("No missing block files to load.");
        return ImmutableList.of();
      }
      if (!storageList.containsAll(cacheList)) {
        cacheList.removeAll(storageList);
        LOGGER.error("Cache list contains references to files that no longer exist {}", cacheList);
        throw new IOException("Missing files error.");
      }
      _blockFiles.set(ImmutableList.copyOf(storageList));
    }

    List<Path> newFiles = new ArrayList<>(storageList);
    newFiles.removeAll(cacheList);
    LOGGER.info("New files found.");
    for (Path path : storageList) {
      LOGGER.info("Loading {}.", path);
      getReader(path);
    }

    return newFiles;
  }

  public int getFileSystemBlockSize() {
    return _fileSystemBlockSize;
  }

  public long getMaxMemory() {
    return _maxMemory;
  }

  @Override
  public void close() throws IOException {
    _hdfsKeyValueStore.sync();
    _hdfsKeyValueStore.close();
    _hdfsKeyValueTimer.cancel();
    _hdfsKeyValueTimer.purge();
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
    int blockSize = _fileSystemBlockSize;
    try {

      int blockOffset = (int) (position % blockSize);
      long blockId = getBlockId(position);
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
      _hdfsKeyValueStore.put(blockId, byteBuffer);
      return len;
    } finally {
      writeBlockIfNeeded(); // run as back ground thread
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int len) throws IOException {
    int blockSize = _fileSystemBlockSize;

    ByteBuffer byteBuffer = ByteBuffer.allocate(blockSize);
    long blockId = getBlockId(position);
    if (!_hdfsKeyValueStore.get(blockId, byteBuffer)) {
      readBlocks(blockId, byteBuffer);
    }
    int blockOffset = (int) (position % blockSize);
    int length = Math.min(len, blockSize - blockOffset);
    if (byteBuffer.limit() == 0) {
      System.arraycopy(_emptyBlock, 0, buffer, offset, length);
    } else if (byteBuffer.limit() == blockSize) {
      System.arraycopy(byteBuffer.array(), blockOffset, buffer, offset, length);
    } else {
      throw new IOException("Bytebuffer limit " + byteBuffer.limit() + " does not equal " + blockSize);
    }
    return length;
  }

  @Override
  public void fsync() throws IOException {
    _hdfsKeyValueStore.sync();
  }

  public long getKeyStoreMemoryUsage() {
    return _hdfsKeyValueStore.getSizeOfData();
  }

  protected Path getHdfsBlockPath(long hdfsBlock) {
    return qualify(new Path(_path, "block." + Long.toString(hdfsBlock)));
  }

  private synchronized void writeBlockIfNeeded() throws IOException {
    if (_hdfsKeyValueStore.getSizeOfData() >= _maxMemory
        || _hdfsKeyValueStore.getNumberOfEntries() >= _maxMemoryEntries) {
      LOGGER.debug("Writing block, memory size {} entries {}", _hdfsKeyValueStore.getSizeOfData(),
          _hdfsKeyValueStore.getNumberOfEntries());

      _hdfsKeyValueStore.writeExternal(getExternalWriter(), true);

      LOGGER.debug("After writing block, memory size {} entries {}", _hdfsKeyValueStore.getSizeOfData(),
          _hdfsKeyValueStore.getNumberOfEntries());
    }
  }

  private ExternalWriter getExternalWriter() throws IOException {
    Path path = qualify(new Path(_blockPath, UUID.randomUUID()
                                                 .toString()
        + ".tmp"));
    Writer writer = BlockFile.create(_fileSystem, path, _fileSystemBlockSize);
    return new ExternalWriter() {
      @Override
      public void write(long key, BytesWritable writable) throws IOException {
        writer.append(key, writable);
      }

      @Override
      public void commit() throws IOException {
        writer.close();
        Path blockPath = getNewBlockFilePath();
        checkIfStillOwner();
        if (_fileSystem.rename(path, blockPath)) {
          getReader(blockPath);// open file ahead of time
          Builder<Path> builder = ImmutableList.builder();
          builder.add(blockPath);
          synchronized (_blockFiles) {
            List<Path> list = _blockFiles.get();
            if (list != null) {
              builder.addAll(list);
            }
            _blockFiles.set(builder.build());
          }
        } else {
          throw new IOException("Could not commit tmp block " + path + " to " + blockPath);
        }
      }
    };
  }

  protected void checkIfStillOwner() throws IOException {
    if (!_hdfsKeyValueStore.isOwner()) {
      throw new IOException("This instance is no longer the owner of " + _path);
    }
  }

  private Path getNewBlockFilePath() {
    return qualify(new Path(_blockPath, System.currentTimeMillis() + "." + BLOCK));
  }

  private void readBlocks(long blockId, ByteBuffer byteBuffer) throws IOException {
    BytesWritable value = new BytesWritable();
    List<Path> list = _blockFiles.get();
    if (list != null) {
      for (Path path : list) {
        Reader reader = getReader(path);
        if (reader.read(blockId, value)) {
          byteBuffer.put(value.getBytes(), 0, value.getLength());
          return;
        }
      }
    }
  }

  private Reader getReader(Path path) throws IOException {
    try {
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
  public LinuxFileSystem getLinuxFileSystem() {
    return _metaData.getFileSystemType()
                    .getLinuxFileSystem();
  }

}
