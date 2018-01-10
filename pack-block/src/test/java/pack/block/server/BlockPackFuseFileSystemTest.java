package pack.block.server;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.Md5Utils;
import com.google.common.collect.ImmutableMap;

import pack.block.blockstore.compactor.PackCompactorServer;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.util.HdfsSnapshotUtil;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.util.Utils;
import pack.zk.utils.ZkMiniCluster;

public class BlockPackFuseFileSystemTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackFuseFileSystemTest.class);

  private static final String USER_NAME = "user.name";
  private static final String TEST_BLOCK_PACK_FUSE_FILE_SYSTEM = "testBlockPackFuseFileSystem";
  private static final String CACHE = "cache";
  private static final String RW = "rw";
  private static final String HDFS = "hdfs";
  private static final String ZK = "zk";
  private static final String METRICS = "metrics";
  private static final String MOUNT = "mount";
  private static final String FUSE = "fuse";
  private static final int MAX_FILE_SIZE = 100_000;
  private static final int MAX_PASSES = 1000;
  private static final int MIN_PASSES = 100;
  private static final int MAX_BUFFER_SIZE = 16000;
  private static final int MIN_BUFFER_SIZE = 1000;
  private static MiniDFSCluster cluster;
  private static FileSystem fileSystem;
  private static File root = new File("./target/tmp/" + BlockPackFuseFileSystemTest.class.getName());
  private static ZkMiniCluster zkMiniCluster;
  private static String zkConnection;
  private static int zkTimeout;
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

    File zk = Utils.mkdir(new File(root, ZK));
    zkMiniCluster = new ZkMiniCluster();
    zkMiniCluster.startZooKeeper(zk.getAbsolutePath(), true);
    zkConnection = zkMiniCluster.getZkConnectionString();
    zkTimeout = 10000;
    seed = new Random().nextLong();
  }

  @AfterClass
  public static void teardown() {
    cluster.shutdown();
    zkMiniCluster.shutdownZooKeeper();
  }

  @Test
  public void testBlockPackFuseFileSystem() throws Exception {
    File fuse = new File(root, TEST_BLOCK_PACK_FUSE_FILE_SYSTEM);
    Path volumePath = new Path("/BlockPackFuseTest/" + TEST_BLOCK_PACK_FUSE_FILE_SYSTEM);
    fileSystem.delete(volumePath, true);
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .length(100L * 1024L * 1024L * 1024L)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, volumePath);
    HdfsSnapshotUtil.createSnapshot(fileSystem, volumePath, HdfsSnapshotUtil.getMountSnapshotName());
    HdfsBlockStoreConfig config = HdfsBlockStoreConfig.DEFAULT_CONFIG;
    File fuseDir = new File(fuse, FUSE);
    String fuseLocalPath = Utils.mkdir(fuseDir)
                                .getAbsolutePath();
    String fsLocalPath = Utils.mkdir(new File(fuse, MOUNT))
                              .getAbsolutePath();
    String fsLocalCachePath = Utils.mkdir(new File(fuse, CACHE))
                                   .getAbsolutePath();
    String metricsLocalPath = Utils.mkdir(new File(fuse, METRICS))
                                   .getAbsolutePath();

    BlockPackAdmin blockPackAdmin = new BlockPackAdmin() {
    };
    BlockPackFuseConfig fuseConfig = BlockPackFuseConfig.builder()
                                                        .blockPackAdmin(blockPackAdmin)
                                                        .ugi(UserGroupInformation.getCurrentUser())
                                                        .fileSystem(fileSystem)
                                                        .path(volumePath)
                                                        .config(config)
                                                        .fuseLocalPath(fuseLocalPath)
                                                        .fsLocalPath(fsLocalPath)
                                                        .metricsLocalPath(metricsLocalPath)
                                                        .fsLocalCache(fsLocalCachePath)
                                                        .zkConnectionString(zkConnection)
                                                        .zkSessionTimeout(zkTimeout)
                                                        .fileSystemMount(true)
                                                        .blockStoreFactory(BlockStoreFactory.DEFAULT)
                                                        .build();

    File compactorDir = new File(fuse, "compactor");
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("/BlockPackFuseTest"));
    AtomicBoolean running = new AtomicBoolean(true);
    try (PackCompactorServer packCompactorServer = new PackCompactorServer(compactorDir, fileSystem, pathList,
        zkConnection, zkTimeout)) {
      Thread thread = new Thread(() -> {
        while (running.get()) {
          try {
            packCompactorServer.executeCompaction();
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
          } catch (Exception e) {
            LOGGER.error("Unknown error", e);
          }
        }
      });
      thread.start();

      int numberOfFiles = 100;
      int threads = 20;

      ExecutorService pool = Executors.newCachedThreadPool();

      String user = System.getProperty(USER_NAME);

      ImmutableMap.Builder<File, Future<Map<String, String>>> builder = ImmutableMap.builder();
      ImmutableMap<File, Future<Map<String, String>>> map;
      try (BlockPackFuse blockPackFuse = new BlockPackFuse(fuseConfig)) {
        blockPackFuse.mount(false);
        Utils.exec(LOGGER, "sudo", "chown", "-R", user + ":" + user, fsLocalPath);
        File baseDir = new File(fsLocalPath);
        for (int pass = 0; pass < threads; pass++) {
          File testDir = new File(baseDir, Integer.toString(pass));
          Future<Map<String, String>> future = pool.submit(() -> testFileSystem(testDir, numberOfFiles));
          builder.put(testDir, future);
        }
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);
        map = builder.build();
        for (Entry<File, Future<Map<String, String>>> e : map.entrySet()) {
          File testDir = e.getKey();
          Future<Map<String, String>> future = e.getValue();
          validateHashes(testDir, future.get());
        }
      }

      try (BlockPackFuse blockPackFuse = new BlockPackFuse(fuseConfig)) {
        blockPackFuse.mount(false);
        for (Entry<File, Future<Map<String, String>>> e : map.entrySet()) {
          File testDir = e.getKey();
          Future<Map<String, String>> future = e.getValue();
          validateHashes(testDir, future.get());
        }
      }

      running.set(false);
      thread.interrupt();
      thread.join();
    }
  }

  private void validateHashes(File testDir, Map<String, String> hashes) throws IOException {
    for (Entry<String, String> entry : hashes.entrySet()) {
      String hash = entry.getValue();
      File file = new File(testDir, entry.getKey());
      assertEquals(hash, Md5Utils.md5AsBase64(file));
    }
  }

  private Map<String, String> testFileSystem(File baseDir, int numberOfFiles) throws IOException {
    baseDir.mkdirs();
    Map<String, String> fileHashes = new ConcurrentHashMap<>();
    Random random = new Random(seed);
    for (int i = 0; i < numberOfFiles; i++) {
      String fileName = Long.toString(random.nextLong());
      long fileLength = random.nextInt(MAX_FILE_SIZE);
      String md5 = generateFile(random, baseDir, fileName, (int) fileLength);
      fileHashes.put(fileName, md5);
    }
    return fileHashes;
  }

  private String generateFile(Random random, File baseDir, String fileName, int fileLength) throws IOException {
    File file = new File(baseDir, fileName);
    long passSeed = random.nextLong();
    int passes = random.nextInt(MAX_PASSES - MIN_PASSES) + MIN_PASSES;
    int maxBuf = random.nextInt(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE) + MIN_BUFFER_SIZE;
    try (RandomAccessFile rand = new RandomAccessFile(file, RW)) {
      rand.setLength(fileLength);
      writeRandomFile(rand, fileLength, passSeed, passes, maxBuf);
    }
    return Md5Utils.md5AsBase64(file);
  }

  private void writeRandomFile(RandomAccessFile rand, int fileLength, long passSeed, int passes, int maxBuf)
      throws IOException {
    Random random = new Random(passSeed);
    for (int pass = 0; pass < passes; pass++) {
      long pos = random.nextInt(fileLength);
      int length = random.nextInt(maxBuf);
      int len = (int) Math.min(length, fileLength - pos);
      byte[] buf = new byte[len];
      random.nextBytes(buf);
      rand.seek(pos);
      rand.write(buf);
    }
  }

}
