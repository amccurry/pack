package pack.block.blockstore.hdfs.blockstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

import pack.block.blockstore.compactor.BlockFileCompactor;
import pack.block.blockstore.compactor.WalToBlockFileConverter;
import pack.block.blockstore.hdfs.HdfsBlockStoreAdmin;
import pack.block.blockstore.hdfs.HdfsBlockStoreConfig;
import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImpl;

public class HdfsBlockStoreImplTest {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./target/tmp/HdfsBlockStoreV4Test");
  private static FileSystem fileSystem;
  private static MetricRegistry metrics = new MetricRegistry();

  @BeforeClass
  public static void beforeClass() throws IOException {
    Configuration configuration = new Configuration();
    String storePath = new File(storePathDir, "hdfs").getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();
  }

  @AfterClass
  public static void afterClass() {
    cluster.shutdown();
  }

  @Test
  public void testServer() throws Exception {
    Path path = new Path("/test");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .length(100000000)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, path);
    Random random = new Random(1);

    try (HdfsBlockStoreImpl store = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path)) {
      long s = System.nanoTime();
      for (int i = 0; i < 10000; i++) {
        int blockSize = store.getFileSystemBlockSize();
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

    new File("");

    int blockSize = metaData.getFileSystemBlockSize();
    byte[] buffer = new byte[blockSize];
    Arrays.fill(buffer, (byte) 1);

    try (HdfsBlockStoreImpl store = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path)) {
      long position = 0;
      for (int j = 0; j < 1000; j++) {
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

    try (BlockFileCompactor compactor = new BlockFileCompactor(fileSystem, path, newMetaData, null)) {
      compactor.runCompaction();
    }

    try (HdfsBlockStoreImpl store = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path)) {
      store.processBlockFiles();
    }

    FileStatus[] fileStatus = fileSystem.listStatus(new Path(path, HdfsBlockStoreConfig.BLOCK));
    assertEquals(1, fileStatus.length);

    for (FileStatus status : fileStatus) {
      assertTrue(status.getPath()
                       .getName()
                       .endsWith(HdfsBlockStoreConfig.BLOCK));
    }
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

    try (HdfsBlockStoreImpl store = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path)) {
      for (int j = 0; j < 10000; j++) {
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

    try (HdfsBlockStoreImpl store = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path)) {
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

    try (HdfsBlockStoreImpl store = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path)) {
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
    try (HdfsBlockStoreImpl store = new HdfsBlockStoreImpl(metrics, getCacheDir(), fileSystem, path)) {
      long s = System.nanoTime();
      for (int i = 0; i < passes; i++) {
        int blockSize = store.getFileSystemBlockSize();
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
    try (BlockFileCompactor compactor = new BlockFileCompactor(fileSystem, path, metaData, null)) {
      compactor.runCompaction();
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
        int blockSize = store.getFileSystemBlockSize();
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
