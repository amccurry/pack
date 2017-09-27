package pack.block.blockstore.compactor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BytesWritable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.io.Closer;

import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;
import pack.block.blockstore.hdfs.file.BlockFile.Writer;

public class BlockFileCompactorTest {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./test");
  private static File cacheDir = new File(storePathDir, "cache");
  private static FileSystem fileSystem;
  private static long seed;

  @BeforeClass
  public static void beforeClass() throws IOException {
    Configuration configuration = new Configuration();
    String storePath = storePathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();
    seed = new Random().nextLong();
  }

  @AfterClass
  public static void afterClass() {
    cluster.shutdown();
  }

  private int maxPases = 2;

  @Test
  public void testCompactorSingleResultBlockSize() throws IOException {
    Path path = new Path("/testCompactorSingleResultBlockSize");
    Random random = new Random(seed);
    for (int pass = 0; pass < maxPases; pass++) {
      Path basePath = new Path(path, Integer.toString(pass));
      fileSystem.mkdirs(basePath);
      runCompactorSingleResultBlockSize(basePath, random);
    }
  }

  @Test
  public void testCompactorSingleResultObsoleteRatio() throws IOException {
    Path path = new Path("/testCompactorSingleResultObsoleteRatio");
    Random random = new Random(seed);
    for (int pass = 0; pass < maxPases; pass++) {
      Path basePath = new Path(path, Integer.toString(pass));
      fileSystem.mkdirs(basePath);
      runCompactorSingleResultObsoleteRatio(basePath, random);
    }
  }

  @Test
  public void testCompactorMultipleResults() throws IOException {
    Path path = new Path("/testCompactorMultipleResults");
    Random random = new Random(seed);
    for (int pass = 0; pass < maxPases; pass++) {
      Path basePath = new Path(path, Integer.toString(pass));
      fileSystem.mkdirs(basePath);
      runCompactorMultipleResults(basePath, random);
    }
  }

  private void runCompactorSingleResultObsoleteRatio(Path path, Random random) throws IOException {
    int blockSize = 10;
    List<byte[]> data = new ArrayList<>();

    int maxFiles = 30;
    int maxBlockIdsIncr = 100;
    int maxNumberOfBlocksToWrite = 100;
    generatBlockFiles(data, path, random, blockSize, maxFiles, maxBlockIdsIncr, maxNumberOfBlocksToWrite);

    HdfsMetaData newMetaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                             .maxBlockFileSize(0)
                                                             .maxObsoleteRatio(0.0)
                                                             .build();

    try (BlockFileCompactor compactor = new BlockFileCompactor(cacheDir, fileSystem, path, newMetaData, null)) {
      compactor.runCompaction();
    }

    dropOldBlockFiles(path);

    Path blockFile = getSingleBlockFile(path);
    logicallyAssertEquals(data, Arrays.asList(blockFile));
  }

  private void runCompactorSingleResultBlockSize(Path path, Random random) throws IOException {
    int blockSize = 10;
    List<byte[]> data = new ArrayList<>();

    int maxFiles = 30;
    int maxBlockIdsIncr = 100;
    int maxNumberOfBlocksToWrite = 100;
    generatBlockFiles(data, path, random, blockSize, maxFiles, maxBlockIdsIncr, maxNumberOfBlocksToWrite);

    HdfsMetaData newMetaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                             .maxBlockFileSize(Long.MAX_VALUE)
                                                             .maxObsoleteRatio(10.0)
                                                             .build();

    try (BlockFileCompactor compactor = new BlockFileCompactor(cacheDir, fileSystem, path, newMetaData, null)) {
      compactor.runCompaction();
    }

    dropOldBlockFiles(path);

    Path blockFile = getSingleBlockFile(path);
    logicallyAssertEquals(data, Arrays.asList(blockFile));
  }

  private void runCompactorMultipleResults(Path path, Random random) throws IOException {
    int blockSize = 10;
    List<byte[]> data = new ArrayList<>();

    int maxFilesUnderBlockFileSize = 50;
    int maxFilesOverBlockFileSize = 7;
    int maxBlockIdsIncr = 100;
    int maxNumberOfBlocksToWrite = 100;
    long maxBlockFileSize = 100000;
    generatBlockFilesNotConsidered(data, path, random, blockSize, maxFilesUnderBlockFileSize, maxFilesOverBlockFileSize,
        maxBlockIdsIncr, maxNumberOfBlocksToWrite, maxBlockFileSize);

    HdfsMetaData newMetaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                             .maxBlockFileSize(maxBlockFileSize)
                                                             .maxObsoleteRatio(10.0)
                                                             .build();

    try (BlockFileCompactor compactor = new BlockFileCompactor(cacheDir, fileSystem, path, newMetaData, null)) {
      compactor.runCompaction();
    }

    dropOldBlockFiles(path);

    List<Path> blockFiles = getMultipleBlockFiles(path);
    logicallyAssertEquals(data, blockFiles);
  }

  private void logicallyAssertEquals(List<byte[]> data, List<Path> blockFiles) throws IOException {
    try (Closer closer = Closer.create()) {
      List<Reader> readers = new ArrayList<>();
      ArrayList<Path> sortedList = new ArrayList<>(blockFiles);
      Collections.sort(sortedList, BlockFile.ORDERED_PATH_COMPARATOR);
      for (Path blockFile : sortedList) {
        readers.add(closer.register(BlockFile.open(fileSystem, blockFile)));
      }
      for (int id = 0; id < data.size(); id++) {
        byte[] bs = data.get(id);
        String message = "Block id " + id;
        if (bs == null) {
          // System.out.println("Checking - missing block " + i);
          // Missing block, no empty writes or data writes
          assertFalse(message, doesBlockExistence(readers, id));
          assertFalse(message, isBlockEmpty(readers, id));
        } else {
          if (isAllZeros(bs)) {
            // System.out.println("Checking - all zeros block " + i);
            assertFalse(message, doesBlockExistence(readers, id));
            assertTrue(message, isBlockEmpty(readers, id));
          } else {
            // System.out.println("Checking - data block " + i);
            assertTrue(message, doesBlockExistence(readers, id));
            assertFalse(message, isBlockEmpty(readers, id));

            BytesWritable value = new BytesWritable();
            readValue(readers, id, value);
            assertEquals(message, value.getLength(), bs.length);
            assertTrue(message, Arrays.equals(value.copyBytes(), bs));
          }
        }
      }
    }

  }

  private void readValue(List<Reader> readers, int i, BytesWritable value) throws IOException {
    for (Reader reader : readers) {
      if (reader.read(i, value)) {
        return;
      }
    }
  }

  private boolean isBlockEmpty(List<Reader> readers, int i) {
    for (Reader reader : readers) {
      if (reader.hasBlock(i)) {
        return false;
      } else if (reader.hasEmptyBlock(i)) {
        return true;
      }
    }
    return false;
  }

  private boolean doesBlockExistence(List<Reader> readers, int i) {
    for (Reader reader : readers) {
      if (reader.hasEmptyBlock(i)) {
        return false;
      } else if (reader.hasBlock(i)) {
        return true;
      }
    }
    return false;
  }

  private List<Path> getMultipleBlockFiles(Path path) throws IOException {
    Path blockPath = new Path(path, HdfsBlockStoreConfig.BLOCK);
    FileStatus[] listStatus = fileSystem.listStatus(blockPath);
    Builder<Path> builder = ImmutableList.builder();
    for (FileStatus status : listStatus) {
      builder.add(status.getPath());
    }
    return builder.build();
  }

  private void generatBlockFilesNotConsidered(List<byte[]> data, Path path, Random random, int blockSize,
      int maxFilesUnderBlockFileSize, int maxFilesOverBlockFileSize, int maxBlockIdsIncr, int maxNumberOfBlocksToWrite,
      long maxBlockFileSize) throws IOException {

    Path blockPath = new Path(path, HdfsBlockStoreConfig.BLOCK);
    fileSystem.mkdirs(blockPath);

    int numberFilesUnderBlockFileSize = random.nextInt(maxFilesUnderBlockFileSize - 1) + 1;
    int numberFilesOverBlockFileSize = random.nextInt(maxFilesOverBlockFileSize - 1) + 1;
    int under = 0;
    int over = 0;
    for (int i = 0; i < numberFilesUnderBlockFileSize + numberFilesOverBlockFileSize; i++) {
      if (random.nextBoolean()) {
        if (under >= numberFilesUnderBlockFileSize) {
          continue;
        }
        int numberOfBlocksToWrite = random.nextInt(maxNumberOfBlocksToWrite - 1) + 1;
        generatBlockFile(data, blockPath, random, blockSize, maxBlockIdsIncr, numberOfBlocksToWrite, -1L);
        under++;
      } else {
        if (over >= numberFilesOverBlockFileSize) {
          continue;
        }
        generatBlockFile(data, blockPath, random, blockSize, maxBlockIdsIncr, -1, maxBlockFileSize + blockSize);
        over++;
      }
    }
  }

  private void dropOldBlockFiles(Path root) throws IOException {
    Path blockPath = new Path(root, HdfsBlockStoreConfig.BLOCK);
    FileStatus[] listStatus = fileSystem.listStatus(blockPath);
    for (FileStatus status : listStatus) {
      if (!fileSystem.exists(status.getPath())) {
        continue;
      }
      try (Reader reader = BlockFile.open(fileSystem, status.getPath())) {
        List<String> sourceBlockFiles = reader.getSourceBlockFiles();
        if (sourceBlockFiles != null) {
          removeBlockFiles(blockPath, sourceBlockFiles);
        }
      }
    }
  }

  private void removeBlockFiles(Path blockPath, List<String> sourceBlockFiles) throws IOException {
    for (String name : sourceBlockFiles) {
      Path path = new Path(blockPath, name);
      if (!fileSystem.exists(path)) {
        return;
      }
      fileSystem.delete(path, false);
    }
  }

  private Path getSingleBlockFile(Path path) throws IOException {
    Path blockPath = new Path(path, HdfsBlockStoreConfig.BLOCK);
    FileStatus[] listStatus = fileSystem.listStatus(blockPath);
    assertEquals(1, listStatus.length);
    return listStatus[0].getPath();
  }

  private void generatBlockFiles(List<byte[]> data, Path path, Random random, int blockSize, int maxFiles,
      int maxBlockIdsIncr, int maxNumberOfBlocksToWrite) throws IOException {
    Path blockPath = new Path(path, HdfsBlockStoreConfig.BLOCK);
    fileSystem.mkdirs(blockPath);

    int numberOfFilesToProduce = random.nextInt(maxFiles);
    for (int i = 0; i < numberOfFilesToProduce; i++) {
      int numberOfBlocksToWrite = random.nextInt(maxNumberOfBlocksToWrite - 1) + 1;
      generatBlockFile(data, blockPath, random, blockSize, maxBlockIdsIncr, numberOfBlocksToWrite, -1L);
    }
  }

  private boolean isAllZeros(byte[] bs) {
    for (int i = 0; i < bs.length; i++) {
      if (bs[i] != 0) {
        return false;
      }
    }
    return true;
  }

  private Path generatBlockFile(List<byte[]> data, Path blockPath, Random random, int blockSize, int maxBlockIdsIncr,
      int numberOfBlocksToWrite, long minLength) throws IOException {
    Path file = new Path(blockPath, System.currentTimeMillis() + ".block");
    try (Writer writer = BlockFile.create(true, fileSystem, file, blockSize)) {
      int blockId = random.nextInt(maxBlockIdsIncr);
      for (int b = 0; b < numberOfBlocksToWrite || writer.getLen() < minLength; b++) {
        growIfNeeded(data, blockId);
        byte[] blockData = new byte[blockSize];
        if (!random.nextBoolean()) {
          // System.out.println("Adding - data block " + blockId);
          random.nextBytes(blockData);
        } else {
          // System.out.println("Adding - empty block " + blockId);
        }
        data.set(blockId, blockData);
        writer.append(blockId, bw(blockData));
        blockId = random.nextInt(maxBlockIdsIncr - 1) + 1 + blockId;
      }
    }
    FileStatus fileStatus = fileSystem.getFileStatus(file);
    System.out.println("File " + file.getName() + " " + fileStatus.getLen());
    return file;
  }

  private void growIfNeeded(List<byte[]> data, int blockId) {
    while (data.size() <= blockId) {
      data.add(null);
    }
  }

  private BytesWritable bw(byte[] data) {
    return new BytesWritable(data);
  }

}
