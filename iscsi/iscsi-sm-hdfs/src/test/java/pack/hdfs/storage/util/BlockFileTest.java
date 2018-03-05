package pack.hdfs.storage.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BytesWritable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import pack.hdfs.storage.util.BlockFile.BlockFileEntry;
import pack.hdfs.storage.util.BlockFile.Reader;
import pack.hdfs.storage.util.BlockFile.ReaderMultiOrdered;
import pack.hdfs.storage.util.BlockFile.Writer;
import pack.hdfs.storage.util.BlockFile.WriterMultiOrdered;
import pack.hdfs.storage.util.BlockFile.WriterOrdered;

public class BlockFileTest {

  private static final File TMPDIR = new File(System.getProperty("hdfs.tmp.dir", "./target/tmp_BlockFileTest"));

  private static Configuration _configuration = new Configuration();
  private static MiniDFSCluster _cluster;

  @BeforeClass
  public static void startCluster() {
    _cluster = HdfsMiniClusterUtil.startDfs(_configuration, true, TMPDIR.getAbsolutePath());
  }

  @AfterClass
  public static void stopCluster() {
    HdfsMiniClusterUtil.shutdownDfs(_cluster);
  }

  @Test
  public void testBlockFile() throws IOException {
    Path path = new Path("/testBlockFile");
    FileSystem fileSystem = _cluster.getFileSystem();
    int valueLength = 100;
    int checkCount = 1000;
    System.out.println("Writing");
    try (Writer writer = BlockFile.create(true, fileSystem, path, valueLength)) {
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
    try (Writer writer = BlockFile.create(true, fileSystem, path, valueLength)) {
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
    try (Writer writer = BlockFile.create(true, fileSystem, path, valueLength)) {
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
    try (Writer writer = BlockFile.create(true, fileSystem, path, valueLength)) {
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

    Path path2 = new Path("/2.block");
    writeBlockFile(path2, vl, kv(1));

    Path path1 = new Path("/1.block");
    writeBlockFile(path1, vl, kv(1, vl), kv(3), kv(5, vl));

    Path path0 = new Path("/0.block");
    writeBlockFile(path0, vl, kv(1, vl), kv(2), kv(4, vl));

    Path path = new Path("/testBlockFileMergeOutput");
    FileSystem fileSystem = _cluster.getFileSystem();

    List<Reader> readers = new ArrayList<>();
    readers.add(BlockFile.open(fileSystem, path2));
    readers.add(BlockFile.open(fileSystem, path1));
    readers.add(BlockFile.open(fileSystem, path0));

    try (WriterOrdered writer = BlockFile.createOrdered(fileSystem, path, vl)) {
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

  @Test
  public void testReadRequestsFromMultiOrderedWriter() throws IOException {
    Path path = new Path("/testReadRequestsFromMultiOrderedWriter");
    FileSystem fileSystem = _cluster.getFileSystem();
    int valueLength = 100;
    int checkCount = 1000;
    int maxSkip = 10;
    int startingPoint = 1000000;
    long seed = 4;
    System.out.println("Writing");
    try (Writer writer = BlockFile.create(false, fileSystem, path, valueLength)) {
      Random random = new Random(seed);
      long longKey = random.nextInt(startingPoint);
      for (int i = 0; i < checkCount; i++) {
        BytesWritable value = getValue(longKey, valueLength);
        writer.append(longKey, value);
        System.out.println("======================");
        System.out.println(longKey);
        System.out.println(value);
        longKey = random.nextInt(maxSkip - 1) + 1;
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
          longKey = random.nextInt(maxSkip - 1) + 1;
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
          longKey = random.nextInt(maxSkip - 1) + 1;
        }
      }
    }
  }

  @Test
  public void testReaderMultiOrdered() throws IOException {
    Path path = new Path("/testReaderMultiOrdered");
    FileSystem fileSystem = _cluster.getFileSystem();
    int valueLength = 100;
    byte[] buf = new byte[valueLength];
    Random random = new Random();
    random.nextBytes(buf);

    BytesWritable value = new BytesWritable(new byte[valueLength]);
    Path file = new Path(path, "testfile");

    WriterMultiOrdered writer = (WriterMultiOrdered) BlockFile.create(false, fileSystem, file, valueLength);
    writer.append(1, new BytesWritable(buf));
    writer.append(2, new BytesWritable(buf));
    writer.writeFooter();
    long sync1 = writer.sync();

    ReaderMultiOrdered reader = BlockFile.openMultiOrdered(fileSystem, file, sync1);

    assertTrue(reader.hasBlock(1));
    assertTrue(reader.hasBlock(2));

    reader.read(1, value);
    assertTrue(Arrays.equals(value.getBytes(), buf));
    reader.read(2, value);
    assertTrue(Arrays.equals(value.getBytes(), buf));

    writer.append(0, new BytesWritable(buf));
    writer.writeFooter();
    long sync2 = writer.sync();
    ReaderMultiOrdered reopen = reader.reopen(fileSystem, sync2);
    reader.close();

    assertTrue(reopen.hasBlock(0));
    assertTrue(reopen.hasBlock(1));
    assertTrue(reopen.hasBlock(2));

    reopen.read(1, value);
    assertTrue(Arrays.equals(value.getBytes(), buf));
    reopen.read(2, value);
    assertTrue(Arrays.equals(value.getBytes(), buf));
    reopen.read(3, value);
    assertTrue(Arrays.equals(value.getBytes(), buf));
  }

  @Test
  public void testBlockFileMergeRandom() throws IOException {
    int vl = 10;
    int maxLayers = 200;
    int maxKeysPerLayer = 200;
    int maxKeyValues = 1_000_000;
    long seed = getSeed();
    System.out.println("Test seed " + seed);

    Random random = new Random(seed);
    int layers = random.nextInt(maxLayers);

    try (RandomAccessFile rand = new RandomAccessFile(new File("./target/testBlockFileMergeRandom.data"), "rw")) {
      RoaringBitmap allKeys = new RoaringBitmap();
      rand.setLength(vl * maxKeyValues);
      for (int layer = 0; layer < layers; layer++) {
        RoaringBitmap keys = getKeys(random, maxKeyValues, maxKeysPerLayer);
        allKeys.or(keys);
        Path path = new Path("/" + layer + ".block");
        FileSystem fileSystem = _cluster.getFileSystem();
        try (Writer writer = BlockFile.create(true, fileSystem, path, vl)) {
          for (Integer key : keys) {
            long pos = ((int) key) * (long) vl;
            KeyValue kv = kv(key, vl, random);
            rand.seek(pos);
            rand.write(kv.value.getBytes());
            if (kv.value == null) {
              writer.appendEmpty(kv.key);
            } else {
              writer.append(kv.key, kv.value);
            }
          }
        }
      }

      Path path = new Path("/testBlockFileMergeRandom");
      FileSystem fileSystem = _cluster.getFileSystem();
      PathFilter pathFilter = (PathFilter) path1 -> path1.getName()
                                                         .endsWith(".block");
      FileStatus[] listStatus = fileSystem.listStatus(new Path("/"), pathFilter);
      List<Reader> readers = new ArrayList<>();
      for (FileStatus fileStatus : listStatus) {
        readers.add(BlockFile.open(fileSystem, fileStatus.getPath()));
      }
      try (WriterOrdered writer = BlockFile.createOrdered(fileSystem, path, vl)) {
        BlockFile.merge(readers, writer);
      }
      readers.forEach(reader -> IOUtils.closeQuietly(reader));

      Reader reader = BlockFile.open(fileSystem, path);
      RoaringBitmap dataKeys = new RoaringBitmap();
      reader.orDataBlocks(dataKeys);

      assertEquals(allKeys, dataKeys);

      byte[] buffer = new byte[vl];
      BytesWritable value = new BytesWritable();
      for (Integer key : dataKeys) {
        assertTrue(reader.hasBlock(key));
        long pos = ((int) key) * (long) vl;
        rand.seek(pos);
        rand.read(buffer);
        reader.read(key, value);
        assertEquals(vl, value.getLength());
        assertEquals(new BytesWritable(buffer), value);
      }
    }
  }

  private long getSeed() {
    Random random = new Random();
    long seed = random.nextLong();
    return seed;
  }

  private RoaringBitmap getKeys(Random random, int maxKeyValues, int maxKeysPerLayer) {
    int numberOfKeys = random.nextInt(maxKeysPerLayer);
    RoaringBitmap keys = new RoaringBitmap();
    for (int i = 0; i < numberOfKeys; i++) {
      keys.add(random.nextInt(maxKeyValues));
    }
    return keys;
  }

  private KeyValue kv(int key, int valuelength, Random random) {
    return new KeyValue(key, valuelength, random);
  }

  private KeyValue kv(int key) {
    return new KeyValue(key);
  }

  private KeyValue kv(int key, int valuelength) {
    return new KeyValue(key, valuelength);
  }

  private void writeBlockFile(Path path, int valueLength, KeyValue... keyValues) throws IOException {
    FileSystem fileSystem = _cluster.getFileSystem();
    try (Writer writer = BlockFile.create(true, fileSystem, path, valueLength)) {
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
      this(key, valuelength, null);
    }

    public KeyValue(int key, int valuelength, Random random) {
      this.key = key;
      this.value = createBytesWritable(valuelength, random);

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
    return createBytesWritable(valuelength, null);
  }

  private static BytesWritable createBytesWritable(int valuelength, Random random) {
    byte[] bs = new byte[valuelength];
    if (random == null) {
      Arrays.fill(bs, (byte) 'a');
    } else {
      random.nextBytes(bs);
    }
    return new BytesWritable(bs);
  }
}
