package pack.block.blockstore.compactor;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;

import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.blockstore.LocalWalCache;
import pack.block.blockstore.hdfs.blockstore.WalFileFactory;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;
import pack.block.blockstore.hdfs.file.BlockFile.Writer;
import pack.block.blockstore.hdfs.file.ReadRequest;
import pack.block.blockstore.hdfs.lock.HdfsLock;
import pack.block.blockstore.hdfs.lock.LockLostAction;
import pack.block.util.Utils;

public class WalToBlockFileConverter implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(WalToBlockFileConverter.class);

  private static final String CONVERT = "0_convert";
  private static final Joiner JOINER = Joiner.on('.');
  private static final Splitter SPLITTER = Splitter.on('.');
  private final Path _blockPath;
  private final FileSystem _fileSystem;
  private final Cache<Path, Reader> _readerCache;
  private final int _blockSize;
  private final AtomicLong _length;
  private final File _cacheDir;
  private final WalFileFactory _walFactory;
  private final String _nodePrefix;
  private final Path _volumePath;
  private final boolean _useLock;

  public WalToBlockFileConverter(File cacheDir, FileSystem fileSystem, Path volumePath, HdfsMetaData metaData,
      boolean useLock) throws IOException {
    _useLock = useLock;
    _nodePrefix = InetAddress.getLocalHost()
                             .getHostName();
    _cacheDir = cacheDir;
    _length = new AtomicLong(metaData.getLength());
    _blockSize = metaData.getFileSystemBlockSize();
    _fileSystem = fileSystem;
    _volumePath = volumePath;
    _blockPath = new Path(volumePath, HdfsBlockStoreConfig.BLOCK);
    _walFactory = WalFileFactory.create(_fileSystem, metaData);
    cleanupOldFiles();
    RemovalListener<Path, BlockFile.Reader> listener = notification -> IOUtils.closeQuietly(notification.getValue());
    _readerCache = CacheBuilder.newBuilder()
                               .removalListener(listener)
                               .build();
  }

  @Override
  public void close() throws IOException {
    _readerCache.invalidateAll();
  }

  public void runConverter() throws IOException, InterruptedException {
    if (!_fileSystem.exists(_blockPath)) {
      LOGGER.info("Path {} does not exist, exiting", _blockPath);
      return;
    }
    convertWalFiles();
  }

  private void convertWalFiles() throws IOException, InterruptedException {
    FileStatus[] listStatus = _fileSystem.listStatus(_blockPath, (PathFilter) path -> path.getName()
                                                                                          .endsWith(".wal"));
    for (FileStatus fileStatus : listStatus) {
      // add logging and check on wal file....
      Path walPath = fileStatus.getPath();

      if (_useLock) {
        Path path = Utils.getLockPathForVolume(_volumePath, walPath.getName());
        LockLostAction lockLostAction = () -> {
          LOGGER.error("Lock lost for wal {}", path);
        };
        try (HdfsLock lock = new HdfsLock(_fileSystem.getConf(), path, lockLostAction)) {
          if (lock.tryToLock()) {
            convertWalFile(walPath);
          } else {
            LOGGER.info("Skipping convert no lock {}", path);
          }
        }
      } else {
        convertWalFile(walPath);
      }
    }
  }

  private void convertWalFile(Path path) throws IOException {
    List<String> list = SPLITTER.splitToList(path.getName());
    if (list.size() != 2) {
      throw new IOException("Wal file " + path + " name is malformed.");
    }
    String blockName = JOINER.join(list.get(0), HdfsBlockStoreConfig.BLOCK);
    Path newPath = Utils.qualify(_fileSystem, new Path(path.getParent(), blockName));
    Path tmpPath = Utils.qualify(_fileSystem, new Path(_blockPath, getRandomTmpNameConvert()));
    if (_fileSystem.exists(newPath)) {
      return;
    }
    File dir = new File(_cacheDir, "convertWal-" + path.getName());
    dir.mkdirs();
    File file = new File(dir, UUID.randomUUID()
                                  .toString()
        + ".context");

    try (LocalWalCache localContext = new LocalWalCache(file, _length, _blockSize)) {
      LocalWalCache.applyWal(_walFactory, path, localContext);

      LOGGER.info("Wal convert - Starting to write block");

      try (Writer writer = BlockFile.create(true, _fileSystem, tmpPath, _blockSize, ImmutableList.of(path.getName()),
          () -> {
            LOGGER.info("Wal convert complete path {}", tmpPath);
            if (_fileSystem.rename(tmpPath, newPath)) {
              LOGGER.info("Wal convert commit path {}", newPath);
            } else {
              throw new IOException("Wal convert commit failed");
            }
          })) {

        RoaringBitmap allBlocks = new RoaringBitmap();
        allBlocks.or(localContext.getDataBlocks());
        RoaringBitmap emptyBlocks = localContext.getEmptyBlocks();
        allBlocks.or(emptyBlocks);
        IntConsumer ic = value -> {
          try {
            appendBlock(localContext, writer, emptyBlocks, value);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };
        allBlocks.forEach(ic);
      }
    }
    Utils.rmr(dir);
  }

  private void appendBlock(LocalWalCache localContext, Writer writer, RoaringBitmap emptyBlocks, int value)
      throws IOException {
    if (emptyBlocks.contains(value)) {
      writer.appendEmpty(value);
    } else {
      writer.append(value, getValue(value, localContext));
    }
  }

  private BytesWritable getValue(int blockId, LocalWalCache localContext) throws IOException {
    ByteBuffer dest = ByteBuffer.allocate(_blockSize);
    ReadRequest request = new ReadRequest(blockId, 0, dest);
    if (localContext.readBlock(request)) {
      throw new IOException("Could not find blockid " + blockId);
    }
    dest.flip();
    return Utils.toBw(dest);
  }

  private String getRandomTmpNameConvert() {
    String uuid = UUID.randomUUID()
                      .toString();
    return JOINER.join(getFilePrefix(), uuid);
  }

  private String getFilePrefix() {
    return CONVERT + "." + _nodePrefix;
  }

  class CompactionJob {

    RoaringBitmap _blocksToIgnore;
    List<Path> _pathList = new ArrayList<>();

    public List<Path> getPathListToCompact() {
      return ImmutableList.copyOf(_pathList);
    }

    public RoaringBitmap getBlocksToIgnore() {
      return _blocksToIgnore;
    }

    void add(Path path) {
      _pathList.add(path);
    }

    void finishSetup(RoaringBitmap blocksToIgnore) {
      _blocksToIgnore = blocksToIgnore.clone();
    }

    void addCurrentBlocksForThisCompaction(RoaringBitmap bitSet) throws IOException {
      for (Path p : _pathList) {
        Reader reader = getReader(p);
        reader.orDataBlocks(bitSet);
        reader.orEmptyBlocks(bitSet);
      }
    }

  }

  private Reader getReader(Path path) throws IOException {
    try {
      return _readerCache.get(path, () -> BlockFile.openForStreaming(_fileSystem, path));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  private void cleanupOldFiles() throws IOException {
    if (!_fileSystem.exists(_blockPath)) {
      return;
    }
    FileStatus[] listStatus = _fileSystem.listStatus(_blockPath);
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      if (shouldCleanupFile(path)) {
        LOGGER.info("Deleting old temp merge file {}", path);
        _fileSystem.delete(path, false);
      }
    }
  }

  private boolean shouldCleanupFile(Path path) {
    String name = path.getName();
    return name.startsWith(getFilePrefix());
  }

}
