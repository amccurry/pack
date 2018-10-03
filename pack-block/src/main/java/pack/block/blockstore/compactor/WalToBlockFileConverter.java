package pack.block.blockstore.compactor;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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
import com.google.common.collect.ImmutableList;

import pack.block.blockstore.BlockStoreMetaData;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImplConfig;
import pack.block.blockstore.hdfs.blockstore.wal.LocalWalCache;
import pack.block.blockstore.hdfs.blockstore.wal.WalFileFactory;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Writer;
import pack.block.blockstore.hdfs.file.ReadRequest;
import pack.block.blockstore.hdfs.lock.LockLostAction;
import pack.block.blockstore.hdfs.lock.PackLock;
import pack.block.blockstore.hdfs.lock.PackLockFactory;
import pack.block.util.Utils;

public class WalToBlockFileConverter implements Closeable {

  private static final String WAL = "wal";

  private static final Logger LOGGER = LoggerFactory.getLogger(WalToBlockFileConverter.class);

  private static final String CONVERT = "0_convert";
  private static final Joiner JOINER = Joiner.on('.');
  private static final Splitter SPLITTER = Splitter.on('.');
  private static final String NODE_PREFIX;
  static {
    try {
      NODE_PREFIX = InetAddress.getLocalHost()
                               .getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
  private final Path _blockPath;
  private final FileSystem _fileSystem;
  private final int _blockSize;
  private final AtomicLong _length;
  private final File _cacheDir;
  private final WalFileFactory _walFactory;
  private final Path _volumePath;
  private final boolean _useLock;

  public WalToBlockFileConverter(File cacheDir, FileSystem fileSystem, Path volumePath, BlockStoreMetaData metaData,
      boolean useLock) throws IOException {
    _useLock = useLock;
    _cacheDir = cacheDir;
    _length = new AtomicLong(metaData.getLength());
    _blockSize = metaData.getFileSystemBlockSize();
    _fileSystem = fileSystem;
    _volumePath = volumePath;
    _blockPath = new Path(volumePath, HdfsBlockStoreImplConfig.BLOCK);
    _walFactory = WalFileFactory.create(_fileSystem, metaData);
    cleanupOldFiles();
  }

  @Override
  public void close() throws IOException {
  }

  public void runConverter() throws IOException, InterruptedException {
    if (!_fileSystem.exists(_blockPath)) {
      LOGGER.info("Path {} does not exist, exiting", _blockPath);
      return;
    }

    if (_useLock) {
      Path path = Utils.getLockPathForVolume(_volumePath, WAL);
      LockLostAction lockLostAction = () -> {
        LOGGER.error("Lock lost for wal {}", path);
      };
      try (PackLock lock = PackLockFactory.create(_fileSystem.getConf(), path, lockLostAction)) {
        if (lock.tryToLock()) {
          convertWalFiles();
        } else {
          LOGGER.info("Skipping convert no lock {}", path);
        }
      }
    } else {
      convertWalFiles();
    }
  }

  private void convertWalFiles() throws IOException, InterruptedException {
    FileStatus[] listStatus = _fileSystem.listStatus(_blockPath, (PathFilter) path -> path.getName()
                                                                                          .endsWith(".wal"));
    List<FileStatus> list = new ArrayList<>(Arrays.asList(listStatus));
    // Sorted order for reads
    Collections.sort(list, BlockFile.ORDERED_FILESTATUS_COMPARATOR);
    // Reverse order converting oldest to newest
    Collections.reverse(list);
    for (FileStatus fileStatus : list) {
      convertWalFile(fileStatus.getPath());
    }
  }

  private void convertWalFile(Path path) throws IOException {
    String blockName = getBlockNameFromWalPath(path);
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

    long layer = BlockFile.getLayer(newPath);
    try (LocalWalCache localContext = new LocalWalCache(file, _length, _blockSize, layer)) {
      LocalWalCache.applyWal(_walFactory, path, localContext);
      LOGGER.info("Wal convert - Starting to write block");
      writeNewBlockFromWalCache(_fileSystem, path, newPath, tmpPath, localContext, _blockSize);
    }
    Utils.rmr(dir);
  }

  public static String getBlockNameFromWalPath(Path path) throws IOException {
    List<String> list = SPLITTER.splitToList(path.getName());
    if (list.size() != 2) {
      throw new IOException("Wal file " + path + " name is malformed.");
    }
    return JOINER.join(list.get(0), HdfsBlockStoreImplConfig.BLOCK);
  }

  public static void writeNewBlockFromWalCache(FileSystem fileSystem, Path sourceWalPath, Path newBlockPath,
      Path tmpBlockPath, LocalWalCache localContext, int blockSize) throws IOException {
    try (Writer writer = BlockFile.create(true, fileSystem, tmpBlockPath, blockSize,
        ImmutableList.of(sourceWalPath.getName()), () -> {
          LOGGER.info("Wal convert complete path {}", tmpBlockPath);
          if (fileSystem.rename(tmpBlockPath, newBlockPath)) {
            LOGGER.info("Wal convert commit path {}", newBlockPath);
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
          appendBlock(localContext, writer, emptyBlocks, value, blockSize);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
      allBlocks.forEach(ic);
    }
  }

  private static void appendBlock(LocalWalCache localContext, Writer writer, RoaringBitmap emptyBlocks, int value,
      int blockSize) throws IOException {
    if (emptyBlocks.contains(value)) {
      writer.appendEmpty(value);
    } else {
      writer.append(value, getValue(blockSize, value, localContext));
    }
  }

  private static BytesWritable getValue(int blockSize, int blockId, LocalWalCache localContext) throws IOException {
    ByteBuffer dest = ByteBuffer.allocate(blockSize);
    ReadRequest request = new ReadRequest(blockId, 0, dest);
    if (localContext.readBlock(request)) {
      throw new IOException("Could not find blockid " + blockId);
    }
    dest.flip();
    return Utils.toBw(dest);
  }

  public static String getRandomTmpNameConvert() {
    String uuid = UUID.randomUUID()
                      .toString();
    return JOINER.join(getFilePrefix(), uuid);
  }

  private static String getFilePrefix() {
    return CONVERT + "." + NODE_PREFIX;
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
