package pack.block.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import pack.block.blockstore.BlockStoreMetaData;
import pack.block.blockstore.compactor.BlockFileCompactor;
import pack.block.blockstore.compactor.WalToBlockFileConverter;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.lock.OwnerCheck;
import pack.block.blockstore.hdfs.util.HdfsSnapshotStrategy;
import pack.block.util.Utils;
import pack.util.ExecUtil;
import pack.util.LogLevel;

public class BlockPackStorageTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackStorageTest.class);

  private static final String FUSE = "fuse";
  private static MiniDFSCluster CLUSTER;
  private static DistributedFileSystem FILESYSTEM;
  private static final String HDFS = "hdfs";
  private static File ROOT = new File("./target/tmp/" + BlockPackFuseBlockOnlyTest.class.getName());
  private static File FUSE_FILE;

  @BeforeClass
  public static void setup() throws IOException {
    Utils.rmr(ROOT);
    File storePathDir = Utils.mkdir(new File(ROOT, HDFS));
    FUSE_FILE = Utils.mkdir(new File(ROOT, FUSE));
    Configuration configuration = new Configuration();
    String storePath = storePathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    CLUSTER = new MiniDFSCluster.Builder(configuration).build();
    FILESYSTEM = CLUSTER.getFileSystem();
  }

  @AfterClass
  public static void teardown() {
    CLUSTER.shutdown();
  }

  @Test
  public void testBlockPackStorageWritesAndReadsBlind() throws Throwable {
    int mountPasses = 2;
    int filePasses = 2;
    runIntegrationTest(mountPasses, filePasses, testDir -> {
      File file = new File(testDir, UUID.randomUUID()
                                        .toString());
      int length = 1000000;
      try (RandomAccessFile rand = new RandomAccessFile(file, "rw")) {
        rand.setLength(length);
        byte[] buf = new byte[1024];
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
          int pos = random.nextInt(length - buf.length);
          rand.seek(pos);
          if (random.nextBoolean()) {
            rand.readFully(buf);
          } else {
            random.nextBytes(buf);
            rand.write(buf);
          }
        }
      }
      System.out.println("Finished " + file);
    });
  }

  @Test
  public void testBlockPackStorageWritesAndReadsWithMD5Checks() throws Throwable {
    int mountPasses = 2;
    int filePasses = 2;
    int length = 100000000;
    runIntegrationTest(mountPasses, filePasses, testDir -> {
      File file = new File(testDir, UUID.randomUUID()
                                        .toString());
      int remaining = length;
      byte[] buf = new byte[1024];
      Random random = new Random();

      MessageDigest outputDigest = MessageDigest.getInstance("MD5");
      try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file))) {
        while (remaining > 0) {
          random.nextBytes(buf);
          int len = Math.min(buf.length, remaining);
          output.write(buf, 0, len);
          outputDigest.update(buf, 0, len);
          remaining -= len;
        }
      }
      MessageDigest inputDigest = MessageDigest.getInstance("MD5");
      try (InputStream input = new BufferedInputStream(new FileInputStream(file))) {
        int len;
        while ((len = input.read(buf, 0, buf.length)) != -1) {
          inputDigest.update(buf, 0, len);
        }
      }

      byte[] outputMd5 = outputDigest.digest();
      byte[] inputMd5 = inputDigest.digest();
      if (!Arrays.equals(outputMd5, inputMd5)) {
        throw new IOException("MD5 hashes do not match " + file.getAbsolutePath());
      }
      System.out.println("Finished " + file);
    });
  }

  private void runIntegrationTest(int mountPasses, int filePasses, RunTest test)
      throws IOException, InterruptedException, Exception, Throwable, ExecutionException {
    File workingDir = new File(FUSE_FILE, "workingDir");
    File logDir = new File(FUSE_FILE, "logDir");
    Configuration configuration = FILESYSTEM.getConf();
    Path remotePath = new Path("/pack");

    FILESYSTEM.mkdirs(remotePath);
    FILESYSTEM.setPermission(remotePath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

    int numberOfMountSnapshots = 5;
    HdfsSnapshotStrategy strategy = BlockPackServer.getStrategy();

    BlockPackStorageConfig config = BlockPackStorageConfig.builder()
                                                          .workingDir(workingDir)
                                                          .logDir(logDir)
                                                          .configuration(configuration)
                                                          .remotePath(remotePath)
                                                          .numberOfMountSnapshots(numberOfMountSnapshots)
                                                          .strategy(strategy)
                                                          .build();

    ExecutorService service = Executors.newFixedThreadPool(40);
    File cacheDir = new File(FUSE_FILE, "cache");
    cacheDir.mkdirs();

    AtomicBoolean running = new AtomicBoolean(true);

    String volumeName = "test";

    BlockPackStorage storage = new BlockPackStorage(config);
    storage.create(volumeName, ImmutableMap.of());

    Path volumePath = new Path(remotePath, volumeName);
    BlockStoreMetaData metaData = HdfsBlockStoreAdmin.readMetaData(FILESYSTEM, volumePath);

    Future<Object> compactorFuture = service.submit(() -> {
      OwnerCheck ownerCheck = () -> true;
      while (running.get()) {
        try (BlockFileCompactor compactor = new BlockFileCompactor(FILESYSTEM, volumePath, metaData)) {
          compactor.runCompaction(ownerCheck);
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      }
      return null;
    });

    Future<Object> cleanupFuture = service.submit(() -> {
      while (running.get()) {
        try {
          storage.cleanup();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      }
      return null;
    });

    Future<Object> converterFuture = service.submit(() -> {
      while (running.get()) {
        try (WalToBlockFileConverter converter = new WalToBlockFileConverter(cacheDir, FILESYSTEM, volumePath, metaData,
            false)) {
          converter.runConverter();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      }
      return null;
    });

    try {

      for (int pass = 0; pass < mountPasses; pass++) {
        String id = UUID.randomUUID()
                        .toString();
        try {
          String mount = storage.mount(volumeName, id);
          String username = System.getProperty("user.name");
          ExecUtil.exec(LOGGER, LogLevel.INFO, "sudo", "chown", "-R", username + ":" + username, mount);
          for (int i = 0; i < filePasses; i++) {
            runTest(filePasses, mount, service, test);
          }
        } finally {
          storage.unmount(volumeName, id);
        }
      }
    } finally {
      running.set(false);
      cleanupFuture.get();
      compactorFuture.get();
      converterFuture.get();
      service.shutdownNow();

      storage.cleanup();
    }
  }

  static interface RunTest {
    void performIO(File testDir) throws Exception;
  }

  private void runTest(int numberOfFiles, String mount, ExecutorService service, RunTest test)
      throws InterruptedException, Throwable {
    File testDir = new File(mount, "test");
    testDir.mkdirs();
    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < numberOfFiles; i++) {
      futures.add(service.submit(() -> {
        test.performIO(testDir);
        return null;
      }));
    }
    for (Future<Void> f : futures) {
      try {
        f.get();
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    }
    Utils.rmr(testDir);
  }

}
