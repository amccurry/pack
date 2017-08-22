package pack.block.blockstore.hdfs.file;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Timer;

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
import pack.block.blockstore.hdfs.file.BlockFile;
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
        longKey += random.nextInt(10000);
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
        longKey += random.nextInt(10000);
      }
    }
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

  private BytesWritable getValue(long longKey, int length) {
    Random random = new Random(longKey);
    byte[] buf = new byte[length];
    random.nextBytes(buf);
    return new BytesWritable(buf);
  }

}
