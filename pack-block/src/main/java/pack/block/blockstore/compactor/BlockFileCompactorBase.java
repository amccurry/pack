package pack.block.blockstore.compactor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImplConfig;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;
import pack.block.blockstore.hdfs.file.BlockFile.WriterOrdered;
import pack.block.blockstore.hdfs.lock.OwnerCheck;

public abstract class BlockFileCompactorBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockFileCompactorBase.class);

  private static final String WAL = ".wal";
  private static final String MERGE = "0_merge";
  private static final Joiner JOINER = Joiner.on('.');
  private static final Splitter SPLITTER = Splitter.on('.');

  protected abstract FileSystem getFileSystem() throws IOException;

  protected abstract Path getBlockPath();

  protected abstract Reader getReader(Path path) throws IOException;

  protected abstract String getNodePrefix();

  protected abstract double getMaxObsoleteRatio();

  protected abstract long getMaxBlockFileSize();

  protected abstract int getFileSystemBlockSize();

  public boolean shouldCompact() throws IOException {
    if (!getFileSystem().exists(getBlockPath())) {
      LOGGER.info("Path {} does not exist, exiting", getBlockPath());
      return false;
    }

    FileStatus[] listStatus = getBlockFiles();
    if (listStatus.length < 2) {
      LOGGER.debug("Path {} contains less than 2 block files, exiting", getBlockPath());
      return false;
    }

    Arrays.sort(listStatus, BlockFile.ORDERED_FILESTATUS_COMPARATOR);
    List<CompactionJob> compactionJobs = getCompactionJobs(listStatus);
    if (compactionJobs.isEmpty()) {
      return false;
    }
    return true;
  }

  public void runCompaction(OwnerCheck ownerCheck) throws IOException {
    if (!getFileSystem().exists(getBlockPath())) {
      LOGGER.info("Path {} does not exist, exiting", getBlockPath());
      return;
    }

    FileStatus[] listStatus = getBlockFiles();
    if (listStatus.length < 2) {
      LOGGER.debug("Path {} contains less than 2 block files, exiting", getBlockPath());
      return;
    }

    Arrays.sort(listStatus, BlockFile.ORDERED_FILESTATUS_COMPARATOR);
    List<CompactionJob> compactionJobs = getCompactionJobs(listStatus);
    if (compactionJobs.isEmpty()) {
      return;
    }
    LOGGER.info("Starting compaction - path {} size {}", getBlockPath(),
        getFileSystem().getContentSummary(getBlockPath())
                       .getLength());
    LOGGER.info("Compaction job count {} for path {}", compactionJobs.size(), getBlockPath());
    for (CompactionJob job : compactionJobs) {
      runJob(job, ownerCheck);
    }
    LOGGER.info("Finished compaction - path {} size {}", getBlockPath(),
        getFileSystem().getContentSummary(getBlockPath())
                       .getLength());
  }

  private void runJob(CompactionJob job, OwnerCheck ownerCheck) throws IOException {
    List<Path> pathListToCompact = job.getPathListToCompact();
    if (pathListToCompact.size() < 2) {
      return;
    }

    List<Reader> readers = new ArrayList<>();
    LOGGER.info("Starting compaction {} for path {}", job, getBlockPath());
    List<String> sourceFileList = new ArrayList<>();
    for (Path path : pathListToCompact) {
      LOGGER.info("Adding block file for merge {}", path);
      readers.add(getReader(path));
      sourceFileList.add(path.getName());
    }

    Reader reader = readers.get(0);
    Path newPath = getNewBlockPath(reader.getLogicalPath());
    Path tmpPath = new Path(getBlockPath(), getRandomTmpNameMerge());

    RoaringBitmap blocksToIgnore = job.getBlocksToIgnore();

    LOGGER.info("New merged output path {}", tmpPath);
    try (WriterOrdered writer = BlockFile.createOrdered(getFileSystem(), tmpPath, reader.getBlockSize(), sourceFileList,
        () -> {
          LOGGER.info("Merged complete path {}", tmpPath);
          if (ownerCheck.isLockOwner() && getFileSystem().rename(tmpPath, newPath)) {
            LOGGER.info("Merged commit path {}", newPath);
          } else {
            throw new IOException("Merge failed");
          }
        })) {
      BlockFile.merge(readers, writer, blocksToIgnore);
    }
  }

  private String getRandomTmpNameMerge() {
    String uuid = UUID.randomUUID()
                      .toString();
    return JOINER.join(getFilePrefix(), uuid);
  }

  private String getFilePrefix() {
    return MERGE + "." + getNodePrefix();
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
      LOGGER.info("should compact cal {}", status.getPath());
      if (shouldCompactFile(getFileSystem(), status, getOverallBlocksToIgnore(status, liveFiles), compactionJob)) {
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

  /**
   * In this method we are calculating the existence of all the blocks in newer
   * layers from the provided current layer.
   * 
   * @param status
   * @param listStatus
   * @return
   * @throws IOException
   */
  private RoaringBitmap getOverallBlocksToIgnore(FileStatus currentLayer, List<FileStatus> liveFiles)
      throws IOException {
    List<FileStatus> allLiveFiles = new ArrayList<>(liveFiles);
    Collections.sort(allLiveFiles, BlockFile.ORDERED_FILESTATUS_COMPARATOR);

    RoaringBitmap overallBlocksToIgnore = new RoaringBitmap();
    for (FileStatus fileStatus : liveFiles) {
      if (currentLayer.equals(fileStatus)) {
        break;
      }
      Reader reader = getReader(fileStatus.getPath());
      reader.orDataBlocks(overallBlocksToIgnore);
      reader.orEmptyBlocks(overallBlocksToIgnore);
    }
    return overallBlocksToIgnore;
  }

  private boolean shouldCompactFile(FileSystem fileSystem, FileStatus status, RoaringBitmap overallBlocksToIgnore,
      CompactionJob compactionJob) throws IOException {
    if (checkFileSize(fileSystem, status, overallBlocksToIgnore)) {
      if (!currentJobTooLarge(compactionJob, status.getPath())) {
        return true;
      }
    }
    return false;
  }

  private boolean currentJobTooLarge(CompactionJob compactionJob, Path path) throws IOException {
    List<Path> pathListToCompact = compactionJob.getPathListToCompact();
    Builder<Path> builder = ImmutableList.builder();
    long size = getMergeSizeEstimatedSize(builder.addAll(pathListToCompact)
                                                 .add(path)
                                                 .build());
    LOGGER.info("Calculated size of merge is {}", size);
    return size > getMaxBlockFileSize();
  }

  private long getMergeSizeEstimatedSize(List<Path> blocks) throws IOException {
    List<Path> allLiveFiles = new ArrayList<>(blocks);
    Collections.sort(allLiveFiles, BlockFile.ORDERED_PATH_COMPARATOR);
    RoaringBitmap overallBlocksToInclude = new RoaringBitmap();
    for (Path path : blocks) {
      Reader reader = getReader(path);
      reader.andNotEmptyBlocks(overallBlocksToInclude);
      reader.orDataBlocks(overallBlocksToInclude);
    }
    return overallBlocksToInclude.getLongCardinality() * getFileSystemBlockSize();
  }

  private boolean checkFileSize(FileSystem fileSystem, FileStatus status, RoaringBitmap overallBlocksToIgnore)
      throws IOException {
    Path path = status.getPath();
    long len = BlockFile.getLen(fileSystem, status.getPath());
    if (len < getMaxBlockFileSize()) {
      LOGGER.info("Adding path {} to compaction, because length {} < max length {}", path, status.getLen(),
          getMaxBlockFileSize());
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

    if (obsoleteRatio >= getMaxObsoleteRatio()) {
      LOGGER.info("Adding path {} to compaction, because obsolete ratio {} < max obsolete ratio {}", path,
          obsoleteRatio, getMaxObsoleteRatio());
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
      LOGGER.info("opening reader {}", path);
      Reader reader = getReader(path);
      sourceBlockFiles.addAll(reader.getSourceBlockFiles());
      RoaringBitmap empty = new RoaringBitmap();
      reader.orEmptyBlocks(empty);
      LOGGER.info("reader has empty cardinality {}", empty.getCardinality());
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
    for (FileStatus fileStatus : toBeDeleted.build()) {
      LOGGER.info("File should be deleted, ignoring {}", fileStatus.getPath());
    }
    return builder.build();
  }

  private FileStatus[] getBlockFiles() throws FileNotFoundException, IOException {
    return getBlockFiles(getFileSystem(), getBlockPath());
  }

  public static FileStatus[] getBlockFiles(FileSystem fileSystem, Path path) throws FileNotFoundException, IOException {
    FileStatus[] listStatus = fileSystem.listStatus(path,
        (PathFilter) p -> BlockFile.isOrderedBlock(p) || isWalFile(p));

    Arrays.sort(listStatus, BlockFile.ORDERED_FILESTATUS_COMPARATOR);

    List<FileStatus> list = new ArrayList<>();
    for (int i = listStatus.length - 1; i >= 0; i--) {
      FileStatus fileStatus = listStatus[i];
      LOGGER.info("get block files {}", fileStatus.getPath());
      if (isWalFile(fileStatus.getPath())) {
        break;
      }
      list.add(fileStatus);
    }
    return list.toArray(new FileStatus[list.size()]);
  }

  public static boolean isWalFile(Path p) {
    return p.getName()
            .endsWith(WAL);
  }

  private Path getNewBlockPath(Path path) throws IOException {
    String name = path.getName();
    List<String> list = SPLITTER.splitToList(name);
    String newName;
    if (list.size() == 2) {
      newName = JOINER.join(list.get(0), "0", HdfsBlockStoreImplConfig.BLOCK);
    } else if (list.size() == 3) {
      long gen = Long.parseLong(list.get(1));
      newName = JOINER.join(list.get(0), Long.toString(gen + 1), HdfsBlockStoreImplConfig.BLOCK);
    } else {
      throw new IOException("Path " + path + " invalid");
    }
    return new Path(path.getParent(), newName);
  }

  public void cleanupBlocks() throws IOException {
    if (!getFileSystem().exists(getBlockPath())) {
      return;
    }
    FileStatus[] listStatus = getFileSystem().listStatus(getBlockPath());
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      if (shouldCleanupFile(path)) {
        LOGGER.info("Deleting old temp merge file {}", path);
        getFileSystem().delete(path, false);
      }
    }
  }

  private boolean shouldCleanupFile(Path path) {
    String name = path.getName();
    return name.startsWith(getFilePrefix());
  }
}
