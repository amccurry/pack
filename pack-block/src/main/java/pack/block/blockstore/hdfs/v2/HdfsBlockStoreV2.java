package pack.block.blockstore.hdfs.v2;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import pack.block.server.fs.LinuxFileSystem;
import pack.block.util.Utils;

public class HdfsBlockStoreV2 implements HdfsBlockStore {

  public static final String BLOCK = "block";

  private final static Logger LOGGER = LoggerFactory.getLogger(HdfsBlockStoreV2.class);

  private final FileSystem _fileSystem;
  private final Path _path;
  private final HdfsMetaData _metaData;
  private final long _length;
  private final int _fileSystemBlockSize;
  private final Path _blockPath;
  private final Cache<Path, BlockFile.Reader> _readerCache;
  private final byte[] _emptyBlock;
  private final AtomicReference<List<Path>> _blockFiles = new AtomicReference<>();
  private final Timer _blockFileTimer;
  private final Map<Long, Long> _blockLookup = new ConcurrentHashMap<>();
  private final Object _writeLock = new Object();
  private final AtomicReference<FileRef> fileRef = new AtomicReference<FileRef>();
  private final File _localDir;
  private final RandomAccessFile _raf;
  private final ConcurrentBitSet _cacheIndex;
  private final FileChannel _brickChannel;

  public HdfsBlockStoreV2(File localDir, FileSystem fileSystem, Path path) throws IOException {
    this(localDir, fileSystem, path, HdfsBlockStoreConfig.DEFAULT_CONFIG);
  }

  public HdfsBlockStoreV2(File localDir, FileSystem fileSystem, Path path, HdfsBlockStoreConfig config)
      throws IOException {
    _localDir = localDir;
    _localDir.mkdirs();
    _fileSystem = fileSystem;
    _path = qualify(path);
    _metaData = HdfsBlockStoreAdmin.readMetaData(_fileSystem, _path);

    _fileSystemBlockSize = _metaData.getFileSystemBlockSize();
    _emptyBlock = new byte[_fileSystemBlockSize];

    _length = _metaData.getLength();
    _blockPath = qualify(new Path(_path, BLOCK));
    _fileSystem.mkdirs(_blockPath);
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
    long numberOfBlocks = _metaData.getLength() / _metaData.getFileSystemBlockSize();
    _cacheIndex = new ConcurrentBitSet(numberOfBlocks);
    _raf = new RandomAccessFile(new File(_localDir, "brick_cache"), "rw");
    _brickChannel = _raf.getChannel();
  }

  private Path qualify(Path path) {
    return path.makeQualified(_fileSystem.getUri(), _fileSystem.getWorkingDirectory());
  }

  private List<Path> getBlockFilePathListFromStorage() throws FileNotFoundException, IOException {
    List<Path> pathList = new ArrayList<>();
    FileStatus[] listStatus = _fileSystem.listStatus(_blockPath, (PathFilter) p -> p.getName()
                                                                                    .endsWith("." + BLOCK));
    Arrays.sort(listStatus, BlockFile.ORDERED_FILESTATUS_COMPARATOR);

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
    if (!_fileSystem.exists(path)) {
      return;
    }
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
  private void loadAnyMissingBlockFiles() throws IOException {
    List<Path> storageList;
    List<Path> cacheList;
    synchronized (_blockFiles) {
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
    }

    List<Path> newFiles = new ArrayList<>(storageList);
    newFiles.removeAll(cacheList);
    LOGGER.info("New files found.");
    for (Path path : storageList) {
      LOGGER.info("Loading {}.", path);
      getReader(path);
    }
  }

  public int getFileSystemBlockSize() {
    return _fileSystemBlockSize;
  }

  @Override
  public void close() throws IOException {
    _blockFileTimer.cancel();
    _blockFileTimer.purge();
    _readerCache.invalidateAll();
    _raf.close();
    Utils.rmr(_localDir);
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
    writeInternal(blockId, byteBuffer);
    return len;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int len) throws IOException {
    int blockSize = _fileSystemBlockSize;
    ByteBuffer byteBuffer = ByteBuffer.allocate(blockSize);
    long blockId = getBlockId(position);
    try {
      readInternal(blockId, byteBuffer);
    } catch (ClosedChannelException e) {
      // might have been rolling local file.
      readInternal(blockId, byteBuffer);
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

  private void readInternal(long blockId, ByteBuffer byteBuffer) throws IOException {
    if (!readFromBrickCache(blockId, byteBuffer)) {
      if (!readFromLocalWriteCache(blockId, byteBuffer)) {
        readBlocks(blockId, byteBuffer);
      }
    }
    writeToBrickCache(blockId, byteBuffer);
  }

  private boolean readFromBrickCache(long blockId, ByteBuffer byteBuffer) throws IOException {
    int index = (int) blockId;
    if (_cacheIndex.get(index)) {
      long position = blockId * _fileSystemBlockSize;
      while (byteBuffer.remaining() > 0) {
        position += _brickChannel.read(byteBuffer, position);
      }
      byteBuffer.flip();
      return true;
    }
    return false;
  }

  private void writeToBrickCache(long blockId, ByteBuffer byteBuffer) throws IOException {
    ByteBuffer duplicate = byteBuffer.duplicate();
    long position = blockId * _fileSystemBlockSize;
    while (duplicate.remaining() > 0) {
      position += _brickChannel.write(duplicate, position);
    }
    int index = (int) blockId;
    _cacheIndex.set(index);
  }

  private void writeInternal(long blockId, ByteBuffer byteBuffer) throws IOException {
    synchronized (_writeLock) {
      ByteBuffer duplicate1 = byteBuffer.duplicate();
      FileChannel fileChannel = getFileChannel();
      long position = fileChannel.position();
      while (duplicate1.remaining() > 0) {
        fileChannel.write(duplicate1);
      }
      _blockLookup.put(blockId, position);
      writeToBrickCache(blockId, byteBuffer);
    }
  }

  private boolean readFromLocalWriteCache(long blockId, ByteBuffer byteBuffer) throws IOException {
    Long position = _blockLookup.get(blockId);
    if (position == null) {
      return false;
    }
    FileChannel fileChannel = getFileChannel();
    long pos = position;
    while (byteBuffer.remaining() > 0) {
      int read = fileChannel.read(byteBuffer, pos);
      pos += read;
    }
    byteBuffer.flip();
    return true;
  }

  static class FileRef implements Closeable {

    final File file;
    final RandomAccessFile raf;
    final FileChannel channel;

    public FileRef(File file) throws IOException {
      this.file = file;
      raf = new RandomAccessFile(file, "rw");
      channel = raf.getChannel();
    }

    @Override
    public void close() throws IOException {
      channel.close();
      raf.close();
      file.delete();
    }

  }

  private FileChannel getFileChannel() throws IOException {
    FileRef ref = fileRef.get();
    if (ref != null) {
      return ref.channel;
    }
    return createFileChannel();
  }

  private FileChannel createFileChannel() throws IOException {
    synchronized (fileRef) {
      FileRef ref = fileRef.get();
      if (ref != null) {
        return ref.channel;
      }
      ref = newFileRef();
      fileRef.set(ref);
      return ref.channel;
    }
  }

  private FileRef newFileRef() throws IOException {
    return new FileRef(new File(_localDir, UUID.randomUUID()
                                               .toString()));
  }

  private void localFileReset() throws IOException {
    synchronized (fileRef) {
      FileRef oldRef = fileRef.get();
      FileRef newRef = newFileRef();
      fileRef.set(newRef);
      if (oldRef != null) {
        oldRef.close();
      }
    }
  }

  @Override
  public void fsync() throws IOException {
    Path path = qualify(new Path(_blockPath, UUID.randomUUID()
                                                 .toString()
        + ".tmp"));
    synchronized (_writeLock) {
      try (Writer writer = BlockFile.create(true, _fileSystem, path, _fileSystemBlockSize)) {
        Set<Long> keySet = _blockLookup.keySet();
        List<Long> blockIds = new ArrayList<>(keySet);
        Collections.sort(blockIds);
        ByteBuffer byteBuffer = ByteBuffer.allocate(_fileSystemBlockSize);
        for (Long blockId : blockIds) {
          if (readFromLocalWriteCache(blockId, byteBuffer)) {
            writer.append(blockId, new BytesWritable(byteBuffer.array()));
          }
        }
      }
      Path blockPath = getNewBlockFilePath();
      // Check is still owner.
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
        _blockLookup.clear();
        localFileReset();
      } else {
        throw new IOException("Could not commit tmp block " + path + " to " + blockPath);
      }
    }
  }

  protected Path getHdfsBlockPath(long hdfsBlock) {
    return qualify(new Path(_path, "block." + Long.toString(hdfsBlock)));
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
  public HdfsMetaData getMetaData() {
    return _metaData;
  }

  @Override
  public LinuxFileSystem getLinuxFileSystem() {
    return _metaData.getFileSystemType()
                    .getLinuxFileSystem();
  }

}
