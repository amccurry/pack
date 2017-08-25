package pack.block.blockstore.compactor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.roaringbitmap.RoaringBitmap;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;
import pack.block.blockstore.hdfs.file.BlockFile.Writer;

public class BlockFileCompactorTest {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./test");
  private static FileSystem fileSystem;

  @BeforeClass
  public static void beforeClass() throws IOException {
    Configuration configuration = new Configuration();
    String storePath = storePathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();
  }

  @AfterClass
  public static void afterClass() {
    cluster.shutdown();
  }

  @Test
  public void testCompactorSingleResult() throws IOException {
    Path path = new Path("/testCompactorSingleResult");
    Random random = new Random(3);
    int blockSize = 10;
    List<byte[]> data = new ArrayList<>();

    int maxFiles = 30;
    int maxBlockIdsIncr = 100;
    int maxNumberOfBlocksToWrite = 100;
    generatBlockFiles(data, path, random, blockSize, maxFiles, maxBlockIdsIncr, maxNumberOfBlocksToWrite);

    try (BlockFileCompactor compactor = new BlockFileCompactor(fileSystem, path, Long.MAX_VALUE)) {
      compactor.runCompaction();
    }

    dropOldBlockFiles(path);

    Path blockFile = getSingleBlockFile(path);
    logicallyAssertEquals(data, blockFile);
  }

  private void dropOldBlockFiles(Path root) throws IOException {
    Path blockPath = new Path(root, HdfsBlockStore.BLOCK);
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
    Path blockPath = new Path(path, HdfsBlockStore.BLOCK);
    FileStatus[] listStatus = fileSystem.listStatus(blockPath);
    assertEquals(1, listStatus.length);
    return listStatus[0].getPath();
  }

  private void generatBlockFiles(List<byte[]> data, Path path, Random random, int blockSize, int maxFiles,
      int maxBlockIdsIncr, int maxNumberOfBlocksToWrite) throws IOException {
    Path blockPath = new Path(path, HdfsBlockStore.BLOCK);
    fileSystem.mkdirs(blockPath);

    int numberOfFilesToProduce = random.nextInt(maxFiles);
    for (int i = 0; i < numberOfFilesToProduce; i++) {
      int numberOfBlocksToWrite = random.nextInt(maxNumberOfBlocksToWrite - 1) + 1;
      generatBlockFile(data, blockPath, random, blockSize, maxBlockIdsIncr, numberOfBlocksToWrite);
    }
  }

  private void logicallyAssertEquals(List<byte[]> data, Path blockFile) throws IOException {
    try (Reader reader = BlockFile.open(fileSystem, blockFile)) {
      RoaringBitmap blocks = reader.getBlocks();
      RoaringBitmap emptyBlocks = reader.getEmptyBlocks();
      for (int i = 0; i < data.size(); i++) {

        byte[] bs = data.get(i);
        if (bs == null) {
          System.out.println("Checking - missing block " + i);
          // Missing block, no empty writes or data writes
          assertFalse(blocks.contains(i));
          assertFalse(emptyBlocks.contains(i));
        } else {
          if (isAllZeros(bs)) {
            System.out.println("Checking - all zeros block " + i);
            assertFalse(blocks.contains(i));
            assertTrue(emptyBlocks.contains(i));
          } else {
            System.out.println("Checking - data block " + i);
            assertTrue(blocks.contains(i));
            assertFalse(emptyBlocks.contains(i));

            BytesWritable value = new BytesWritable();
            reader.read(i, value);
            assertEquals(value.getLength(), bs.length);
            assertTrue(Arrays.equals(value.copyBytes(), bs));
          }
        }
      }
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
      int numberOfBlocksToWrite) throws IOException {
    Path file = new Path(blockPath, System.currentTimeMillis() + ".block");
    try (Writer writer = BlockFile.create(fileSystem, file, blockSize)) {
      int blockId = random.nextInt(maxBlockIdsIncr);
      for (int b = 0; b < numberOfBlocksToWrite; b++) {
        growIfNeeded(data, blockId);
        byte[] blockData = new byte[blockSize];
        if (!random.nextBoolean()) {
          System.out.println("Adding - data block " + blockId);
          random.nextBytes(blockData);
        } else {
          System.out.println("Adding - empty block " + blockId);
        }
        data.set(blockId, blockData);
        writer.append(blockId, bw(blockData));
        blockId = random.nextInt(maxBlockIdsIncr - 1) + 1 + blockId;
      }
    }
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
