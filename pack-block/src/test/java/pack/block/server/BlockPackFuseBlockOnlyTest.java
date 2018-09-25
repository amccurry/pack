package pack.block.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import pack.block.blockstore.BlockStoreMetaData;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImplConfig;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.server.json.BlockPackFuseConfig;
import pack.block.server.json.BlockPackFuseConfigInternal;
import pack.block.util.Utils;

public class BlockPackFuseBlockOnlyTest {

  private static final String CACHE = "cache";
  private static final String BRICK = "brick";
  private static final String RW = "rw";
  private static final String HDFS = "hdfs";
  private static final String METRICS = "metrics";
  private static final String FUSE = "fuse";
  private static final String TEST_BLOCK_PACK_FUSE = "testBlockPackFuse";
  private static final int MAX_PASSES = 1000;
  private static final int MIN_PASSES = 100;
  private static final int MAX_BUFFER_SIZE = 16000;
  private static final int MIN_BUFFER_SIZE = 1000;
  private static MiniDFSCluster cluster;
  private static FileSystem fileSystem;
  private static File root = new File("./target/tmp/" + BlockPackFuseBlockOnlyTest.class.getName());
  private static long seed;

  @BeforeClass
  public static void setup() throws IOException {
    Utils.rmr(root);
    File storePathDir = Utils.mkdir(new File(root, HDFS));
    Configuration configuration = new Configuration();
    String storePath = storePathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();
    seed = new Random().nextLong();
  }

  @AfterClass
  public static void teardown() {
    cluster.shutdown();
  }

  interface RunTest {
    void runTest(File fuseDir) throws IOException;
  }

  @Test
  public void testBlockPackFuse() throws Exception {
    runTest(fuseDir -> testFuseMount(fuseDir));
  }

  @Test
  public void testBlockPackFuseRandomAccessTest() throws Exception {
    runTest(fuseDir -> testFuseMountRandomAccess(fuseDir));
  }

  private void testFuseMountRandomAccess(File fuseDir) throws IOException {
    File block = new File(fuseDir, BRICK);
    try (RandomAccessFile rand = new RandomAccessFile(block, "rw")) {
      long value = 0;
      int index = 100;
      for (long l = 0; l < 1000; l++) {
        rand.seek(index);
        rand.writeLong(value);
        rand.seek(index);
        long val = rand.readLong();
        assertEquals(value, val);
        value++;
      }

      // try (FileChannel channel = rand.getChannel()) {
      // MappedByteBuffer mappedByteBuffer = channel.map(MapMode.READ_WRITE, 0,
      // Math.min(Integer.MAX_VALUE, block.length()));
      //
      // long value = 0;
      // int index = 100;
      // for (long l = 0; l < 1000; l++) {
      // mappedByteBuffer.putLong(index, value);
      // channel.force(false);
      // long val = mappedByteBuffer.getLong(index);
      // assertEquals(value, val);
      // value++;
      // }
      // }
    }
  }

  private void runTest(RunTest runTest) throws Exception {
    File fuse = new File(root, TEST_BLOCK_PACK_FUSE);
    Path volumePath = new Path("/BlockPackFuseTest/" + TEST_BLOCK_PACK_FUSE);
    fileSystem.delete(volumePath, true);
    BlockStoreMetaData metaData = BlockStoreMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .length(100 * 1024 * 1024)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, volumePath);
    HdfsBlockStoreImplConfig config = HdfsBlockStoreImplConfig.DEFAULT_CONFIG;
    File fuseDir = new File(fuse, FUSE);
    String fuseLocalPath = Utils.mkdir(fuseDir)
                                .getAbsolutePath();
    String fsLocalCachePath = Utils.mkdir(new File(fuse, CACHE))
                                   .getAbsolutePath();
    String metricsLocalPath = Utils.mkdir(new File(fuse, METRICS))
                                   .getAbsolutePath();

    BlockPackAdmin blockPackAdmin = new BlockPackAdmin() {
    };

    BlockPackFuseConfig packFuseConfig = BlockPackFuseConfig.builder()
                                                            .fuseMountLocation(fuseLocalPath)
                                                            .fsMetricsLocation(metricsLocalPath)
                                                            .fsLocalCache(fsLocalCachePath)
                                                            .build();

    BlockPackFuseConfigInternal fuseConfig = BlockPackFuseConfigInternal.builder()
                                                                        .blockPackAdmin(blockPackAdmin)
                                                                        .fileSystem(fileSystem)
                                                                        .path(volumePath)
                                                                        .config(config)
                                                                        .blockPackFuseConfig(packFuseConfig)
                                                                        .blockStoreFactory(BlockStoreFactory.DEFAULT)
                                                                        .build();

    try (BlockPackFuse blockPackFuse = new BlockPackFuse(fuseConfig)) {
      blockPackFuse.mount(false);
      runTest.runTest(fuseDir);
    }
  }

  private void testFuseMount(File fuseDir) throws IOException {
    File block = new File(fuseDir, BRICK);
    File mirrorFile = new File(root, BRICK + "." + UUID.randomUUID()
                                                       .toString());
    Utils.rmr(mirrorFile);
    int fileLength = (int) block.length();

    Random random = new Random(seed);
    try (RandomAccessFile randBlock = new RandomAccessFile(block, RW)) {
      try (RandomAccessFile mirrorBlock = new RandomAccessFile(mirrorFile, RW)) {
        mirrorBlock.setLength(fileLength);
        for (int i = 0; i < 10; i++) {
          long passSeed = random.nextLong();
          int passes = random.nextInt(MAX_PASSES - MIN_PASSES) + MIN_PASSES;
          int maxBuf = random.nextInt(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE) + MIN_BUFFER_SIZE;
          testFiles(true, fileLength, randBlock, mirrorBlock, passSeed, passes, maxBuf);
          testFiles(false, fileLength, randBlock, mirrorBlock, passSeed, passes, maxBuf);
        }
      }
    }
  }

  private void testFiles(boolean writing, int fileLength, RandomAccessFile randBlock, RandomAccessFile mirrorBlock,
      long passSeed, int passes, int maxBuf) throws IOException {
    Random random = new Random(passSeed);
    for (int pass = 0; pass < passes; pass++) {
      long pos = random.nextInt(fileLength);
      int length = random.nextInt(maxBuf);
      int len = (int) Math.min(length, fileLength - pos);
      if (writing) {
        byte[] buf = new byte[len];
        random.nextBytes(buf);
        randBlock.seek(pos);
        randBlock.write(buf);

        mirrorBlock.seek(pos);
        mirrorBlock.write(buf);

      } else {
        byte[] buf1 = new byte[len];
        randBlock.seek(pos);
        randBlock.readFully(buf1);

        byte[] buf2 = new byte[len];
        mirrorBlock.seek(pos);
        mirrorBlock.readFully(buf2);

        if (!Arrays.equals(buf1, buf2)) {
          System.out.println(Arrays.toString(buf1));
          System.out.println(Arrays.toString(buf2));
        }
        assertTrue("seed [" + seed + "]", Arrays.equals(buf1, buf2));
      }
    }
  }

}
