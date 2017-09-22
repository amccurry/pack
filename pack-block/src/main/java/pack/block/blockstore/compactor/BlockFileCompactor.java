package pack.block.blockstore.compactor;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.zookeeper.KeeperException;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;
import pack.block.blockstore.hdfs.file.BlockFile.Writer;
import pack.block.util.Utils;
import pack.zk.utils.ZooKeeperLockManager;

public class BlockFileCompactor implements Closeable {

  private static final String MERGE = "merge";

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockFileCompactor.class);

  private static final Joiner JOINER = Joiner.on('.');
  private static final Splitter SPLITTER = Splitter.on('.');
  private final Path _blockPath;
  private final FileSystem _fileSystem;
  private final long _maxBlockFileSize;
  private final Cache<Path, Reader> _readerCache;
  private final ZooKeeperLockManager _inUseLockManager;
  private final String _lockName;
  private final double _maxObsoleteRatio;

  public BlockFileCompactor(FileSystem fileSystem, Path path, long maxBlockFileSize, double maxObsoleteRatio,
      ZooKeeperLockManager inUseLockManager) throws IOException {
    _maxObsoleteRatio = maxObsoleteRatio;
    _inUseLockManager = inUseLockManager;
    _maxBlockFileSize = maxBlockFileSize;
    _fileSystem = fileSystem;
    _lockName = Utils.getLockName(path);
    _blockPath = new Path(path, HdfsBlockStoreConfig.BLOCK);
    cleanupBlocks();
    RemovalListener<Path, BlockFile.Reader> listener = notification -> IOUtils.closeQuietly(notification.getValue());
    _readerCache = CacheBuilder.newBuilder()
                               .removalListener(listener)
                               .build();

  }

  @Override
  public void close() throws IOException {
    _readerCache.invalidateAll();
  }

  public void runCompaction() throws IOException {
    if (!_fileSystem.exists(_blockPath)) {
      LOGGER.info("Path {} does not exist, exiting", _blockPath);
      return;
    }
    LOGGER.info("Path {} size {}", _blockPath, _fileSystem.getContentSummary(_blockPath)
                                                          .getLength());
    FileStatus[] listStatus = getBlockFiles();
    if (listStatus.length < 2) {
      LOGGER.info("Path {} contains less than 2 block files, exiting", _blockPath);
      return;
    }
    Arrays.sort(listStatus, Collections.reverseOrder());
    List<CompactionJob> compactionJobs = getCompactionJobs(listStatus);
    if (compactionJobs.isEmpty()) {
      return;
    }
    LOGGER.info("Compaction job count {} for path {}", compactionJobs.size(), _blockPath);
    for (CompactionJob job : compactionJobs) {
      runJob(job);
    }
  }

  private void runJob(CompactionJob job) throws IOException {
    List<Path> pathListToCompact = job.getPathListToCompact();
    if (pathListToCompact.size() < 2) {
      return;
    }

    List<Reader> readers = new ArrayList<>();
    LOGGER.info("Starting compaction {} for path {}", job, _blockPath);
    List<String> sourceFileList = new ArrayList<>();
    for (Path path : pathListToCompact) {
      LOGGER.info("Adding block file for merge {}", path);
      readers.add(getReader(path));
      sourceFileList.add(path.getName());
    }

    Reader reader = readers.get(0);
    Path newPath = getNewPath(reader.getPath());
    Path tmpPath = new Path(_blockPath, getRandomTmpName());

    RoaringBitmap blocksToIgnore = job.getBlocksToIgnore();

    LOGGER.info("New merged output path {}", tmpPath);
    try (Writer writer = BlockFile.create(true, _fileSystem, tmpPath, reader.getBlockSize(), sourceFileList)) {
      BlockFile.merge(readers, writer, blocksToIgnore);
    }

    LOGGER.info("Merged complete path {}", tmpPath);
    if (_fileSystem.rename(tmpPath, newPath)) {
      LOGGER.info("Merged commit path {}", newPath);
    } else {
      throw new IOException("Merge failed");
    }
  }

  private String getRandomTmpName() {
    String uuid = UUID.randomUUID()
                      .toString();
    return JOINER.join(uuid, MERGE);
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

  private List<CompactionJob> getCompactionJobs(FileStatus[] listStatus) throws IOException {
    List<FileStatus> liveFiles = removeOrphanedBlockFiles(listStatus);
    Builder<CompactionJob> builder = ImmutableList.builder();
    CompactionJob compactionJob = new CompactionJob();
    RoaringBitmap currentCompactionBlocksToIgnore = new RoaringBitmap();
    for (FileStatus status : liveFiles) {
      if (shouldCompactFile(status, getNewerBlocksToIgnore(status, listStatus))) {
        compactionJob.add(status.getPath());
      } else {
        finishSetup(builder, compactionJob, currentCompactionBlocksToIgnore);
        compactionJob.addCurrentBlocksForThisCompaction(currentCompactionBlocksToIgnore);

        // add current block file to the ignore list because we are skipping
        // over this segment
        addCurrentBlocks(currentCompactionBlocksToIgnore, status.getPath());
        // new compaction job
        compactionJob = new CompactionJob();
      }
    }
    finishSetup(builder, compactionJob, currentCompactionBlocksToIgnore);
    return builder.build();
  }

  private RoaringBitmap getNewerBlocksToIgnore(FileStatus status, FileStatus[] listStatus) throws IOException {
    FileStatus[] copy = new FileStatus[listStatus.length];
    System.arraycopy(listStatus, 0, copy, 0, listStatus.length);

    Arrays.sort(copy);

    RoaringBitmap overallBlocksToIgnore = new RoaringBitmap();
    boolean cumulate = false;
    for (FileStatus fileStatus : copy) {
      if (cumulate) {
        Reader reader = getReader(fileStatus.getPath());
        reader.orDataBlocks(overallBlocksToIgnore);
        reader.orEmptyBlocks(overallBlocksToIgnore);
      }
      if (status.equals(fileStatus)) {
        cumulate = true;
      }
    }
    return overallBlocksToIgnore;
  }

  private boolean shouldCompactFile(FileStatus status, RoaringBitmap overallBlocksToIgnore) throws IOException {
    Path path = status.getPath();
    if (status.getLen() < _maxBlockFileSize) {
      LOGGER.info("Adding path {} to compaction, because length {} < max length {}", path, status.getLen(),
          _maxBlockFileSize);
      return true;
    }

    Reader reader = getReader(path);
    RoaringBitmap currentReaderBlocks = new RoaringBitmap();
    reader.orDataBlocks(currentReaderBlocks);
    reader.orEmptyBlocks(currentReaderBlocks);

    int currentCardinality = currentReaderBlocks.getCardinality();
    LOGGER.info("Cardinality {} of path {}", currentCardinality, path);

    int cardinality = overallBlocksToIgnore.getCardinality();
    LOGGER.info("Cardinality {} of block to ignore", cardinality);

    currentReaderBlocks.and(overallBlocksToIgnore);
    int obsoleteCardinality = currentReaderBlocks.getCardinality();
    LOGGER.info("Obsolete cardinality {} of path {}", obsoleteCardinality, path);

    double obsoleteRatio = ((double) obsoleteCardinality / (double) currentCardinality);
    LOGGER.info("Obsolete ratio {} of path {}", obsoleteRatio, path);

    if (obsoleteRatio >= _maxObsoleteRatio) {
      LOGGER.info("Adding path {} to compaction, because obsolete ratio {} < max obsolete ratio {}", path,
          obsoleteRatio, _maxObsoleteRatio);
      return true;
    }
    return false;
  }

  private void addCurrentBlocks(RoaringBitmap blocksToIgnore, Path path) throws IOException {
    Reader reader = getReader(path);
    reader.orDataBlocks(blocksToIgnore);
    reader.orEmptyBlocks(blocksToIgnore);
  }

  private void finishSetup(Builder<CompactionJob> builder, CompactionJob compactionJob, RoaringBitmap blocksToIgnore) {
    compactionJob.finishSetup(blocksToIgnore);
    List<Path> pathListToCompact = compactionJob.getPathListToCompact();
    if (pathListToCompact.size() > 1) {
      builder.add(compactionJob);
    }
  }

  private List<FileStatus> removeOrphanedBlockFiles(FileStatus[] listStatus) throws IOException {
    Set<String> sourceBlockFiles = new HashSet<>();
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      Reader reader = getReader(path);
      sourceBlockFiles.addAll(reader.getSourceBlockFiles());
    }

    Builder<FileStatus> toBeDeleted = ImmutableList.builder();
    Builder<FileStatus> builder = ImmutableList.builder();
    for (FileStatus fileStatus : listStatus) {
      String name = fileStatus.getPath()
                              .getName();
      if (!sourceBlockFiles.contains(name)) {
        builder.add(fileStatus);
      } else {
        toBeDeleted.add(fileStatus);
      }
    }
    tryToRemove(toBeDeleted.build());
    return builder.build();
  }

  private void tryToRemove(List<FileStatus> filesToBeDeleted) {
    for (FileStatus fileStatus : filesToBeDeleted) {
      try {
        tryToRemove(fileStatus);
      } catch (KeeperException | InterruptedException | IOException e) {
        LOGGER.error("Unknown error", e);
      }
    }
  }

  private void tryToRemove(FileStatus fileStatus) throws KeeperException, InterruptedException, IOException {
    if (_inUseLockManager == null) {
      return;
    }
    if (_inUseLockManager.tryToLock(_lockName)) {
      try {
        Path path = fileStatus.getPath();
        LOGGER.info("Removing orphaned file {}", path);
        _fileSystem.delete(path, false);
      } finally {
        _inUseLockManager.unlock(_lockName);
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

  private FileStatus[] getBlockFiles() throws FileNotFoundException, IOException {
    return _fileSystem.listStatus(_blockPath, (PathFilter) p -> BlockFile.isOrderedBlock(p));
  }

  private Path getNewPath(Path path) throws IOException {
    String name = path.getName();
    List<String> list = SPLITTER.splitToList(name);
    String newName;
    if (list.size() == 2) {
      newName = JOINER.join(list.get(0), "0", list.get(1));
    } else if (list.size() == 3) {
      long gen = Long.parseLong(list.get(1));
      newName = JOINER.join(list.get(0), Long.toString(gen + 1), list.get(2));
    } else {
      throw new IOException("Path " + path + " invalid");
    }
    return new Path(path.getParent(), newName);
  }

  private void cleanupBlocks() throws IOException {
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
    return path.getName()
               .endsWith(MERGE);
  }
}
