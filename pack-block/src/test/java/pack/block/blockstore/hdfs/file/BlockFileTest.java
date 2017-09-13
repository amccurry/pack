package pack.block.blockstore.hdfs.file;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Timer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BytesWritable;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import pack.block.blockstore.hdfs.HdfsMiniClusterUtil;
import pack.block.blockstore.hdfs.file.BlockFile.BlockFileEntry;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;
import pack.block.blockstore.hdfs.file.BlockFile.Writer;

public class BlockFileTest {

  private static final File TMPDIR = new File(System.getProperty("hdfs.tmp.dir", "./target/tmp_BlockFileTest"));

  private static Configuration _configuration = new Configuration();
  private static MiniDFSCluster _cluster;

  private static Timer _timer;
  private Path _path;

  @BeforeClass
  public static void startCluster() {
    _cluster = HdfsMiniClusterUtil.startDfs(_configuration, true, TMPDIR.getAbsolutePath());
    _timer = new Timer("IndexImporter", true);
  }

  @AfterClass
  public static void stopCluster() {
    _timer.cancel();
    _timer.purge();
    HdfsMiniClusterUtil.shutdownDfs(_cluster);
  }

  @Before
  public void setup() throws IOException {
    FileSystem fileSystem = _cluster.getFileSystem();
    _path = new Path("/test").makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    fileSystem.delete(_path, true);
  }

  @Test
  public void testBlockFile() throws IOException {
    Path path = new Path("/testBlockFile");
    FileSystem fileSystem = _cluster.getFileSystem();
    int valueLength = 100;
    int checkCount = 1000;
    System.out.println("Writing");
    try (Writer writer = BlockFile.create(fileSystem, path, valueLength)) {
      Random random = new Random(4);
      long longKey = random.nextInt(1000000);
      for (int i = 0; i < checkCount; i++) {
        BytesWritable value = getValue(longKey, valueLength);
        writer.append(longKey, value);
        System.out.println("======================");
        System.out.println(longKey);
        System.out.println(value);
        longKey += random.nextInt(10000 - 1) + 1;
      }
    }

    System.out.println("Reading");
    try (Reader reader = BlockFile.open(fileSystem, path)) {
      Random random = new Random(4);
      long longKey = random.nextInt(1000000);
      BytesWritable actual = new BytesWritable();
      for (int i = 0; i < checkCount; i++) {
        BytesWritable expected = getValue(longKey, valueLength);
        assertTrue(reader.read(longKey, actual));
        System.out.println("======================");
        System.out.println(longKey);
        System.out.println(expected);
        System.out.println(actual);
        assertTrue(expected.compareTo(actual) == 0);
        longKey += random.nextInt(10000 - 1) + 1;
      }
    }

  }

  @Test
  public void testReadRequests() throws IOException {
    Path path = new Path("/testReadRequests");
    FileSystem fileSystem = _cluster.getFileSystem();
    int valueLength = 100;
    int checkCount = 1000;
    int maxSkip = 10;
    int startingPoint = 1000000;
    long seed = 4;
    System.out.println("Writing");
    try (Writer writer = BlockFile.create(fileSystem, path, valueLength)) {
      Random random = new Random(seed);
      long longKey = random.nextInt(startingPoint);
      for (int i = 0; i < checkCount; i++) {
        BytesWritable value = getValue(longKey, valueLength);
        writer.append(longKey, value);
        System.out.println("======================");
        System.out.println(longKey);
        System.out.println(value);
        longKey += random.nextInt(maxSkip - 1) + 1;
      }
    }

    System.out.println("Reading Requests");
    try (Reader reader = BlockFile.open(fileSystem, path)) {

      List<BytesWritable> expectedList = new ArrayList<>();
      List<ByteBuffer> actualList = new ArrayList<>();
      List<ReadRequest> requests = new ArrayList<>();

      {
        Random random = new Random(seed);
        long longKey = random.nextInt(startingPoint);

        for (int i = 0; i < checkCount; i++) {
          expectedList.add(getValue(longKey, valueLength));
          ByteBuffer dest = ByteBuffer.allocate(valueLength);
          actualList.add(dest);
          requests.add(new ReadRequest(longKey, 0, dest));
          longKey += random.nextInt(maxSkip - 1) + 1;
        }
      }
      reader.read(requests);
      {
        Random random = new Random(seed);
        long longKey = random.nextInt(startingPoint);

        for (int i = 0; i < checkCount; i++) {
          BytesWritable actual = toBw(actualList.get(i));
          BytesWritable expected = expectedList.get(i);
          assertTrue(requests.get(i)
                             .isCompleted());
          System.out.println("======================");
          System.out.println(longKey);
          System.out.println(expected);
          System.out.println(actual);
          assertTrue(expected.compareTo(actual) == 0);
          longKey += random.nextInt(maxSkip - 1) + 1;
        }
      }
    }
  }

  private BytesWritable toBw(ByteBuffer byteBuffer) {
    return new BytesWritable(byteBuffer.array());
  }

  @Test
  public void testBlockFileOutOfOrderAppends() throws IOException {
    Path path = new Path("/testBlockFileOutOfOrderAppends");
    FileSystem fileSystem = _cluster.getFileSystem();
    int valueLength = 100;
    System.out.println("Writing");
    try (Writer writer = BlockFile.create(fileSystem, path, valueLength)) {
      writer.append(10, getValue(10, valueLength));
      try {
        writer.append(0, getValue(0, valueLength));
        fail();
      } catch (IOException e) {
      }
    }
  }

  @Test
  public void testBlockFileEmptyBlocks() throws IOException {
    Path path = new Path("/testBlockFileEmptyBlocks");
    FileSystem fileSystem = _cluster.getFileSystem();
    int valueLength = 100;
    System.out.println("Writing");
    try (Writer writer = BlockFile.create(fileSystem, path, valueLength)) {
      writer.append(10, new BytesWritable(new byte[valueLength]));
      writer.append(11, new BytesWritable(new byte[0]));
    }
    BytesWritable empty = new BytesWritable(new byte[valueLength]);
    try (Reader reader = BlockFile.open(fileSystem, path)) {
      BytesWritable value = new BytesWritable();
      assertTrue(reader.read(10, value));
      assertEquals(empty, value);
      assertTrue(reader.read(11, value));
      assertEquals(empty, value);
    }
  }

  @Test
  public void testBlockFileMerge() throws IOException {
    int vl = 10;

    Path path0 = new Path("/testBlockFileMerge0");
    writeBlockFile(path0, vl, kv(1));

    Path path1 = new Path("/testBlockFileMerge1");
    writeBlockFile(path1, vl, kv(1, vl), kv(3), kv(5, vl));

    Path path2 = new Path("/testBlockFileMerge2");
    writeBlockFile(path2, vl, kv(1, vl), kv(2), kv(4, vl));

    Path path = new Path("/testBlockFileMergeOutput");
    FileSystem fileSystem = _cluster.getFileSystem();

    List<Reader> readers = new ArrayList<>();
    readers.add(BlockFile.open(fileSystem, path0));
    readers.add(BlockFile.open(fileSystem, path1));
    readers.add(BlockFile.open(fileSystem, path2));

    try (Writer writer = BlockFile.create(fileSystem, path, vl)) {
      BlockFile.merge(readers, writer);
    }
    readers.forEach(reader -> IOUtils.closeQuietly(reader));

    try (Reader reader = BlockFile.open(fileSystem, path)) {
      Iterator<BlockFileEntry> iterator = reader.iterator();
      {
        assertTrue(iterator.hasNext());
        BlockFileEntry bfe1 = iterator.next();
        assertEquals(1, bfe1.getBlockId());
        assertTrue(bfe1.isEmpty());
      }
      {
        assertTrue(iterator.hasNext());
        BlockFileEntry bfe2 = iterator.next();
        assertEquals(2, bfe2.getBlockId());
        assertTrue(bfe2.isEmpty());
      }
      {
        assertTrue(iterator.hasNext());
        BlockFileEntry bfe3 = iterator.next();
        assertEquals(3, bfe3.getBlockId());
        assertTrue(bfe3.isEmpty());
      }
      {
        assertTrue(iterator.hasNext());
        BlockFileEntry bfe4 = iterator.next();
        assertEquals(4, bfe4.getBlockId());
        assertFalse(bfe4.isEmpty());
        BytesWritable bw = new BytesWritable();
        bfe4.readData(bw);
        assertEquals(createBytesWritable(vl), bw);
      }
      {
        assertTrue(iterator.hasNext());
        BlockFileEntry bfe5 = iterator.next();
        assertEquals(5, bfe5.getBlockId());
        assertFalse(bfe5.isEmpty());
        BytesWritable bw = new BytesWritable();
        bfe5.readData(bw);
        assertEquals(createBytesWritable(vl), bw);
      }
    }
  }

  private KeyValue kv(int key) {
    return new KeyValue(key);
  }

  private KeyValue kv(int key, int valuelength) {
    return new KeyValue(key, valuelength);
  }

  private void writeBlockFile(Path path, int valueLength, KeyValue... keyValues) throws IOException {
    FileSystem fileSystem = _cluster.getFileSystem();
    try (Writer writer = BlockFile.create(fileSystem, path, valueLength)) {
      for (KeyValue kv : keyValues) {
        if (kv.value == null) {
          writer.appendEmpty(kv.key);
        } else {
          writer.append(kv.key, kv.value);
        }
      }
    }
  }

  private static class KeyValue {
    public KeyValue(int key) {
      this.key = key;
      this.value = null;
    }

    public KeyValue(int key, int valuelength) {
      this.key = key;
      this.value = createBytesWritable(valuelength);
    }

    final int key;
    final BytesWritable value;
  }

  private BytesWritable getValue(long longKey, int length) {
    Random random = new Random(longKey);
    byte[] buf = new byte[length];
    random.nextBytes(buf);
    return new BytesWritable(buf);
  }

  private static BytesWritable createBytesWritable(int valuelength) {
    byte[] bs = new byte[valuelength];
    Arrays.fill(bs, (byte) 'a');
    return new BytesWritable(bs);
  }
}
