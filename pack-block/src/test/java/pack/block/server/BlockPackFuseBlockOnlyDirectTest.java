package pack.block.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import pack.block.blockstore.BlockStoreMetaData;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImplConfig;
import pack.block.fuse.FuseFileSystemSingleMount;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.server.json.BlockPackFuseConfig;
import pack.block.server.json.BlockPackFuseConfigInternal;
import pack.block.util.Utils;

public class BlockPackFuseBlockOnlyDirectTest {

  private static final String CACHE = "cache";
  private static final String HDFS = "hdfs";
  private static final String METRICS = "metrics";
  private static final String FUSE = "fuse";
  private static final String TEST_BLOCK_PACK_FUSE = "testBlockPackFuse";
  private static MiniDFSCluster cluster;
  private static FileSystem fileSystem;
  private static File root = new File("./target/tmp/" + BlockPackFuseBlockOnlyDirectTest.class.getName());
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

  @Test
  public void testBlockPackFuseDirect() throws Exception {
    File fuse = new File(root, TEST_BLOCK_PACK_FUSE);
    Path volumePath = new Path("/testBlockPackFuseDirect/" + TEST_BLOCK_PACK_FUSE);
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
      Runtime runtime = Runtime.getSystemRuntime();
      FuseFileSystemSingleMount fuseMount = blockPackFuse.getFuse();
      int size = 8;
      int passes = 10_000;
      int threads = 10;

      ExecutorService pool = Executors.newFixedThreadPool(threads);
      try {
        Object lock = new Object();
        List<Future<String>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
          long offset = 0 + (t * 512);
          int run = t;
          Callable<String> callable = () -> {
            Pointer pointer1 = Pointer.wrap(runtime, ByteBuffer.wrap(new byte[16 * 1024]));
            Pointer pointer2 = Pointer.wrap(runtime, ByteBuffer.wrap(new byte[16 * 1024]));

            long newSeed = seed + run;
            String result = "Seed " + seed + " RunSeed " + newSeed + " Offset " + offset;
            System.out.println("Starting " + result);
            Random random = new Random(newSeed);
            for (int i = 0; i < passes; i++) {
              byte[] buf1 = new byte[size];
              putLong(buf1, 0, random.nextLong());
              byte[] buf2 = new byte[size];
              pointer1.put(0, buf1, 0, size);
              synchronized (lock) {
                assertEquals("Seed " + seed + " " + i, size, fuseMount.write("/brick", pointer1, size, offset, null));
                assertEquals("Seed " + seed + " " + i, size, fuseMount.read("/brick", pointer2, size, offset, null));
              }
              pointer2.get(0, buf2, 0, size);
              assertTrue("Seed " + seed + " " + i, Arrays.equals(buf1, buf2));
            }
            System.out.println("Good run!");
            return result;
          };
          futures.add(pool.submit(callable));
        }

        for (Future<String> future : futures) {
          System.out.println(future.get());
        }
      } finally {
        pool.shutdownNow();
      }
    }
  }

  public static void putLong(byte[] b, int off, long val) {
    b[off + 7] = (byte) (val);
    b[off + 6] = (byte) (val >>> 8);
    b[off + 5] = (byte) (val >>> 16);
    b[off + 4] = (byte) (val >>> 24);
    b[off + 3] = (byte) (val >>> 32);
    b[off + 2] = (byte) (val >>> 40);
    b[off + 1] = (byte) (val >>> 48);
    b[off] = (byte) (val >>> 56);
  }

  public static long getLong(byte[] b, int off) {
    return ((b[off + 7] & 0xFFL)) + ((b[off + 6] & 0xFFL) << 8) + ((b[off + 5] & 0xFFL) << 16)
        + ((b[off + 4] & 0xFFL) << 24) + ((b[off + 3] & 0xFFL) << 32) + ((b[off + 2] & 0xFFL) << 40)
        + ((b[off + 1] & 0xFFL) << 48) + (((long) b[off]) << 56);
  }

}
