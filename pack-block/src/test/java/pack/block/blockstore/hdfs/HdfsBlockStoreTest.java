package pack.block.blockstore.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.HdfsMetaData;

public class HdfsBlockStoreTest {

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
  public void testServer() throws Exception {
    Path path = new Path("/test");
    HdfsMetaData metaData = HdfsMetaData.builder()
                                        .length(100000000)
                                        .build();
    HdfsBlockStore.writeHdfsMetaData(metaData, fileSystem, path);
    Random random = new Random();

    try (HdfsBlockStore store = new HdfsBlockStore(fileSystem, path)) {
      for (int i = 0; i < 10000; i++) {
        int blockSize = store.getFileSystemBlockSize();
        int pos = random.nextInt(1000) * blockSize;
        {
          Pointer pointer = Pointer.wrap(Runtime.getSystemRuntime(), ByteBuffer.allocate(blockSize));
          store.read(pos, pointer, 0, blockSize);
        }
        byte[] data = new byte[blockSize];
        random.nextBytes(data);
        {
          ByteBuffer buf = ByteBuffer.allocate(blockSize);
          buf.put(data);
          buf.flip();
          Pointer pointer = Pointer.wrap(Runtime.getSystemRuntime(), buf);
          store.write(pos, pointer, 0, blockSize);
        }
        {
          ByteBuffer buf = ByteBuffer.allocate(blockSize);
          Pointer pointer = Pointer.wrap(Runtime.getSystemRuntime(), buf);
          store.read(pos, pointer, 0, blockSize);
          System.out.println(pos);
          assertTrue(Arrays.equals(data, buf.array()));
        }
      }
    }
  }

  @Test
  public void testEmptyBlock() throws Exception {
    Path path = new Path("/testEmptyBlock");
    HdfsMetaData metaData = HdfsMetaData.builder()
                                        .length(100000000)
                                        .build();
    HdfsBlockStore.writeHdfsMetaData(metaData, fileSystem, path);

    try (HdfsBlockStore store = new HdfsBlockStore(fileSystem, path)) {
      int blockSize = store.getFileSystemBlockSize();
      {
        ByteBuffer buf = ByteBuffer.allocate(blockSize);
        buf.put(new byte[blockSize]);
        buf.flip();
        Pointer pointer = Pointer.wrap(Runtime.getSystemRuntime(), buf);
        store.write(0, pointer, 0, blockSize);
        store.read(0, pointer, 0, blockSize);
      }

      assertEquals(0L, store.getKeyStoreMemoryUsage());

      {
        ByteBuffer buf = ByteBuffer.allocate(blockSize);
        byte[] bs = new byte[blockSize];
        Arrays.fill(bs, (byte) 1);
        buf.put(bs);
        buf.flip();
        Pointer pointer = Pointer.wrap(Runtime.getSystemRuntime(), buf);
        store.write(0, pointer, 0, blockSize);
        store.read(0, pointer, 0, blockSize);
      }

      assertTrue(store.getKeyStoreMemoryUsage() >= blockSize);

      {
        ByteBuffer buf = ByteBuffer.allocate(blockSize);
        buf.put(new byte[blockSize]);
        buf.flip();
        Pointer pointer = Pointer.wrap(Runtime.getSystemRuntime(), buf);
        store.write(0, pointer, 0, blockSize);
        store.read(0, pointer, 0, blockSize);
      }

      assertEquals(0L, store.getKeyStoreMemoryUsage());
    }
  }
}
