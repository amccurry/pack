package pack.block.blockstore.hdfs.blockstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;

import pack.block.blockstore.compactor.BlockFileCompactor;
import pack.block.blockstore.compactor.WalToBlockFileConverter;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.error.RetryBlockStore;

public class RetryHdfsBlockStoreImplTest {

  private static MiniDFSCluster _cluster;
  private static File storePathDir = new File("./target/tmp/HdfsBlockStoreV4Test");
  private static FileSystem fileSystem;
  private static MetricRegistry metrics = new MetricRegistry();
  private static ExecutorService _service = Executors.newSingleThreadExecutor();
  private static Path path = new Path("/shutdowntest");
  private static AtomicBoolean _clusterOnline = new AtomicBoolean();

  @BeforeClass
  public static void beforeClass() throws IOException {
    Configuration configuration = new Configuration();
    String storePath = new File(storePathDir, "hdfs").getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    _cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = _cluster.getFileSystem();
    try (FSDataOutputStream outputStream = fileSystem.create(path)) {
      outputStream.write(1);
    }
    _clusterOnline.set(true);
  }

  @AfterClass
  public static void afterClass() {
    _cluster.shutdown();
    _service.shutdownNow();
  }

  public static void restartCluster() throws InterruptedException {
    System.out.println("shutdown");
    _clusterOnline.set(false);
    _cluster.shutdownDataNodes();

    try {
      System.out.println("wait for error");
      try (FSDataInputStream input = fileSystem.open(path)) {
        System.out.println(input.read());
      }
    } catch (Exception e) {
      System.out.println("cluster down");
    }
    _service.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(5));

          System.out.println("restart");
          _cluster.startDataNodes(fileSystem.getConf(), 1, true, null, null);

          while (!_cluster.isDataNodeUp()) {
            System.out.println("waiting for datanode");
            Thread.sleep(TimeUnit.SECONDS.toMillis(3));
          }

          while (true) {
            try {
              System.out.println("wait for success");
              try (FSDataInputStream input = fileSystem.open(path)) {
                System.out.println(input.read());
              }
              break;
            } catch (Exception e) {
              System.out.println("cluster still down");
            }
          }
          _clusterOnline.set(true);
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    });
  }

  @Test
  public void testServer() throws Exception {
    Path path = new Path("/test");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .length(100000000)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, path);
    Random random = new Random(1);

    HdfsBlockStoreImpl baseStore = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path);
    int blockSize = baseStore.getFileSystemBlockSize();
    try (RetryBlockStore store = RetryBlockStore.wrap(baseStore)) {
      long s = System.nanoTime();
      for (int i = 0; i < 10000; i++) {
        if (i == 2001) {
          restartCluster();
        }

        int pos = random.nextInt(1000) * blockSize;
        {
          byte[] buf = new byte[blockSize];
          store.read(pos, buf, 0, blockSize);
        }
        byte[] data = new byte[blockSize];
        random.nextBytes(data);
        {
          store.write(pos, data, 0, blockSize);
        }
        {
          byte[] buf = new byte[blockSize];
          store.read(pos, buf, 0, blockSize);
          // System.out.println(pos);
          assertTrue(Arrays.equals(data, buf));
        }
      }
      long e = System.nanoTime();
      System.out.println("Run time " + (e - s) / 1_000_000.0 + " ms");
    }
  }

  private File getCacheDir() {
    return new File(storePathDir, "cache");
  }

  @Test
  public void testDeleteBlocks() throws Exception {

    Path path = new Path("/testDeleteBlocks");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .length(100000000)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, path);

    int blockSize = metaData.getFileSystemBlockSize();
    byte[] buffer = new byte[blockSize];
    Arrays.fill(buffer, (byte) 1);

    HdfsBlockStoreImpl baseStore = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path);
    try (RetryBlockStore store = RetryBlockStore.wrap(baseStore)) {
      long position = 0;
      for (int j = 0; j < 1000; j++) {

        if (j == 201) {
          restartCluster();
        }

        if (j % 123 == 0) {
          store.fsync();
        }
        store.write(position, buffer, 0, blockSize);
        position += blockSize;
      }

      store.delete(0, 10000 * blockSize);
    }

    HdfsMetaData newMetaData = metaData.toBuilder()
                                       .maxBlockFileSize(Long.MAX_VALUE)
                                       .maxObsoleteRatio(-0.1)
                                       .build();

    try (WalToBlockFileConverter converter = new WalToBlockFileConverter(new File(storePathDir, "cache"), fileSystem,
        path, newMetaData, null);) {
      converter.runConverter();
    }

    try (BlockFileCompactor compactor = new BlockFileCompactor(fileSystem, path, newMetaData)) {
      compactor.runCompaction(() -> true);
    }

    try (HdfsBlockStoreImpl store = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path)) {
      store.processBlockFiles();
    }

    FileStatus[] fileStatus = fileSystem.listStatus(new Path(path, HdfsBlockStoreConfig.BLOCK),
        (PathFilter) path1 -> path1.getName()
                                   .endsWith(HdfsBlockStoreConfig.BLOCK));
    // there should be 2 blocks because of the cluster restart
    assertEquals(getMessage(fileStatus), 2, fileStatus.length);

    for (FileStatus status : fileStatus) {
      assertTrue(status.getPath()
                       .getName()
                       .endsWith(HdfsBlockStoreConfig.BLOCK));
    }
  }

  private String getMessage(FileStatus[] fileStatus) {
    if (fileStatus == null) {
      return null;
    }
    List<String> list = new ArrayList<>();
    for (FileStatus status : fileStatus) {
      list.add(status.getPath()
                     .toString());
    }
    return Joiner.on(',')
                 .join(list);
  }

  @Test
  public void testReadingWritingNotBlockAligned() throws Exception {
    Path path = new Path("/testReadingWritingNotBlockAligned");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .length(100000000)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, path);

    Random randomSeed = new Random();
    long seed = randomSeed.nextLong();

    Random random = new Random(seed);

    int blockSize = metaData.getFileSystemBlockSize();
    int maxLength = blockSize * 10;
    int maxPos = blockSize * 100;
    int maxOffset = 100;

    HdfsBlockStoreImpl baseStore = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path);
    try (RetryBlockStore store = RetryBlockStore.wrap(baseStore)) {
      for (int j = 0; j < 10000; j++) {

        if (j == 2001) {
          restartCluster();
        }

        int length = random.nextInt(maxLength);
        int offset = random.nextInt(maxOffset);
        byte[] buf1 = new byte[offset + length];

        random.nextBytes(buf1);

        long pos = random.nextInt(maxPos) + 1;

        int writeLength = getLength(pos, length, blockSize);
        assertEquals("Seed " + seed, writeLength, store.write(pos, buf1, offset, length));

        int length2 = length + 2;
        byte[] buf2 = new byte[length2];

        long pos2 = pos - 1;
        assertEquals("Seed " + seed, length2, store.read(pos2, buf2, 0, length2));

        for (int i = 1; i < length2 && i < writeLength + 1; i++) {
          byte expected = buf1[i - 1 + offset];
          byte actual = buf2[i];
          assertEquals("Seed " + seed, expected, actual);
        }
      }
    }
  }

  @Test
  public void testReadingWritingBufferLargerThanBlockSize() throws Exception {
    Path path = new Path("/testReadingWritingBufferLargerThanBlockSize");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .length(100000000)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, path);

    Random random = new Random(1);
    byte[] buf = new byte[122880];
    random.nextBytes(buf);

    {
      HdfsBlockStoreImpl baseStore = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path);
      try (RetryBlockStore store = RetryBlockStore.wrap(baseStore)) {
        {
          long pos = 139264;
          int len = buf.length;
          int offset = 0;
          while (len > 0) {
            int write = store.write(pos, buf, offset, len);
            pos += write;
            offset += write;
            len -= write;
          }
        }

        restartCluster();

        byte[] buf2 = new byte[buf.length];
        {
          long pos = 139264;
          int len = buf.length;
          int offset = 0;
          while (len > 0) {
            int read = store.read(pos, buf2, offset, len);
            pos += read;
            offset += read;
            len -= read;
          }
        }

        assertTrue(Arrays.equals(buf, buf2));
      }
    }
    {
      while (!_clusterOnline.get()) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      }
      HdfsBlockStoreImpl baseStore = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path);
      try (RetryBlockStore store = RetryBlockStore.wrap(baseStore)) {
        byte[] buf2 = new byte[buf.length];
        {
          long pos = 139264;
          int len = buf.length;
          int offset = 0;
          while (len > 0) {
            int read = store.read(pos, buf2, offset, len);
            pos += read;
            offset += read;
            len -= read;
          }
        }

        assertTrue(Arrays.equals(buf, buf2));
      }
    }
  }

  private int getLength(long pos, int length, int blockSize) {
    int blockOffset = (int) (pos % blockSize);
    int len = blockSize - blockOffset;
    return Math.min(len, length);
  }

  @Test
  public void testBlockStoreClone() throws Exception {
    Path path = new Path("/testBlockStoreClone");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .length(100000000)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, path);
    Random random = new Random(1);

    long pos = 0;
    int passes = 1000;
    HdfsBlockStoreImpl baseStore = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path);
    int blockSize = baseStore.getFileSystemBlockSize();
    try (RetryBlockStore store = RetryBlockStore.wrap(baseStore)) {
      long s = System.nanoTime();
      for (int i = 0; i < passes; i++) {
        if (i == 201) {
          restartCluster();
        }

        pos += (random.nextInt(9) + 1) * blockSize;
        {
          byte[] buf = new byte[blockSize];
          store.read(pos, buf, 0, blockSize);
        }
        byte[] data = new byte[blockSize];
        random.nextBytes(data);
        {
          store.write(pos, data, 0, blockSize);
        }
        {
          byte[] buf = new byte[blockSize];
          store.read(pos, buf, 0, blockSize);
          // System.out.println(pos);
          assertTrue(Arrays.equals(data, buf));
        }
      }
      long e = System.nanoTime();
      System.out.println("Run time " + (e - s) / 1_000_000.0 + " ms");
    }
    {
      FileStatus[] listStatus = fileSystem.listStatus(new Path(path, "block"));
      for (FileStatus fileStatus : listStatus) {
        System.out.println(fileStatus.getPath());
      }
    }
    try (WalToBlockFileConverter converter = new WalToBlockFileConverter(getCacheDir(), fileSystem, path, metaData,
        null)) {
      converter.runConverter();
    }
    {
      FileStatus[] listStatus = fileSystem.listStatus(new Path(path, "block"));
      for (FileStatus fileStatus : listStatus) {
        System.out.println(fileStatus.getPath());
      }
    }
    try (BlockFileCompactor compactor = new BlockFileCompactor(fileSystem, path, metaData)) {
      compactor.runCompaction(() -> true);
    }

    Path clonePath = new Path("/testBlockStoreClone2");
    HdfsMetaData cloneMetaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                               .length(100000000)
                                                               .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(cloneMetaData, fileSystem, clonePath);
    HdfsBlockStoreAdmin.clonePath(fileSystem, path, clonePath, true);

    pos = 0;
    random = new Random(1);
    try (HdfsBlockStoreImpl store = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, clonePath)) {
      long s = System.nanoTime();
      for (int i = 0; i < passes; i++) {
        pos += (random.nextInt(9) + 1) * blockSize;
        {
          byte[] buf = new byte[blockSize];
          store.read(pos, buf, 0, blockSize);
        }
        byte[] data = new byte[blockSize];
        random.nextBytes(data);
        {
          // store.write(pos, data, 0, blockSize);
        }
        {
          byte[] buf = new byte[blockSize];
          store.read(pos, buf, 0, blockSize);
          // System.out.println(pos);
          assertTrue(Arrays.equals(data, buf));
        }
      }
      long e = System.nanoTime();
      System.out.println("Run time " + (e - s) / 1_000_000.0 + " ms");
    }
  }

}
