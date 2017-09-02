package pack.block.blockstore.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

public class HdfsBlockStoreTest {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./test");
  private static FileSystem fileSystem;
  private static MetricRegistry metrics = new MetricRegistry();

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
  public void testServer() throws Exception {
    Path path = new Path("/test");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .length(100000000)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, path);
    Random random = new Random();

    try (HdfsBlockStore store = new HdfsBlockStore(metrics, fileSystem, path)) {
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
          System.out.println(pos);
          assertTrue(Arrays.equals(data, buf));
        }
      }
    }
  }

  @Test
  public void testEmptyBlock() throws Exception {
    Path path = new Path("/testEmptyBlock");
    HdfsMetaData metaData = HdfsMetaData.DEFAULT_META_DATA.toBuilder()
                                                          .length(100000000)
                                                          .build();
    HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, path);

    try (HdfsBlockStore store = new HdfsBlockStore(metrics, fileSystem, path)) {
      int blockSize = store.getFileSystemBlockSize();
      {
        byte[] buf = new byte[blockSize];
        store.write(0, buf, 0, blockSize);
        store.read(0, buf, 0, blockSize);
      }

      assertEquals(0L, store.getKeyStoreMemoryUsage());

      {
        byte[] buf = new byte[blockSize];
        Arrays.fill(buf, (byte) 1);
        store.write(0, buf, 0, blockSize);
        store.read(0, buf, 0, blockSize);
      }

      assertTrue(store.getKeyStoreMemoryUsage() >= blockSize);

      {
        byte[] buf = new byte[blockSize];
        store.write(0, buf, 0, blockSize);
        store.read(0, buf, 0, blockSize);
      }

      assertEquals(0L, store.getKeyStoreMemoryUsage());
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

    try (HdfsBlockStore store = new HdfsBlockStore(metrics, fileSystem, path)) {
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
        int readLength = getLength(pos2, length2, blockSize);
        assertEquals("Seed " + seed, readLength, store.read(pos2, buf2, 0, length2));

        for (int i = 1; i < readLength && i < writeLength + 1; i++) {
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

    try (HdfsBlockStore store = new HdfsBlockStore(metrics, fileSystem, path)) {
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
  }

  private int getLength(long pos, int length, int blockSize) {
    int blockOffset = (int) (pos % blockSize);
    int len = blockSize - blockOffset;
    return Math.min(len, length);
  }

}
