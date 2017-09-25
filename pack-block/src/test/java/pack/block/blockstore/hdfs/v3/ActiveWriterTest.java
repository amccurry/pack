package pack.block.blockstore.hdfs.v3;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.WriterMultiOrdered;
import pack.block.blockstore.hdfs.file.CommitFile;
import pack.block.blockstore.hdfs.file.ReadRequest;
import pack.block.util.Utils;

public class ActiveWriterTest {
  private static final String HDFS = "hdfs";
  private static MiniDFSCluster cluster;
  private static FileSystem fileSystem;
  private static File root = new File("./target/tmp/BlockPackFuseTest");

  @BeforeClass
  public static void setup() throws IOException {
    Utils.rmr(root);
    File storePathDir = Utils.mkdir(new File(root, HDFS));
    Configuration configuration = new Configuration();
    String storePath = storePathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();
  }

  @AfterClass
  public static void teardown() {
    cluster.shutdown();
  }

  @Test
  public void testActiveWriter() throws IOException {
    Path dir = new Path("/testActiveWriter");
    Path path = new Path(dir, "log");
    CommitFile commit = () -> fileSystem.rename(path, new Path(dir, "commit"));
    WriterMultiOrdered writer = (WriterMultiOrdered) BlockFile.create(false, fileSystem, path,
        HdfsMetaData.DEFAULT_FILESYSTEM_BLOCKSIZE, commit);
    int maxCommitCount = 10;

    long maxCacheSize = 10 * HdfsMetaData.DEFAULT_FILESYSTEM_BLOCKSIZE;
    int maxCacheCap = 3;

    byte[] buf = new byte[HdfsMetaData.DEFAULT_FILESYSTEM_BLOCKSIZE];
    Arrays.fill(buf, (byte) 1);

    try (ActiveWriter activeWriter = new ActiveWriter(fileSystem, writer, path, maxCommitCount, maxCacheSize,
        maxCacheCap)) {
      {
        activeWriter.append(123, ByteBuffer.wrap(buf));
        preReadActions(activeWriter);
        assertFalse(activeWriter.readCache(getReadrequests(123)));
      }
      {
        activeWriter.append(124, ByteBuffer.wrap(buf));
        preReadActions(activeWriter);
        assertFalse(activeWriter.readCache(getReadrequests(123, 124)));
      }
      {
        activeWriter.append(125, ByteBuffer.wrap(buf));
        preReadActions(activeWriter);
        List<ReadRequest> readrequests = getReadrequests(123, 124, 125);
        assertTrue(activeWriter.readCache(readrequests));
        assertFalse(activeWriter.readCurrentWriteLog(readrequests));
      }
    }

  }

  private void preReadActions(ActiveWriter activeWriter) throws IOException {
    if (activeWriter.hasFlushedCacheBlocks()) {
      if (activeWriter.commit()) {
        activeWriter.close();
      }
    }
  }

  private List<ReadRequest> getReadrequests(long... blockIds) {
    List<ReadRequest> requests = new ArrayList<>();
    for (long blockId : blockIds) {
      requests.add(new ReadRequest(blockId, 0, ByteBuffer.allocate(HdfsMetaData.DEFAULT_FILESYSTEM_BLOCKSIZE)));
    }
    return requests;
  }

}
