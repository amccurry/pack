package pack.iscsi.storage;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import pack.iscsi.kafka.EmbeddedHdfsCluster;
import pack.iscsi.storage.hdfs.BlockFile;
import pack.iscsi.storage.hdfs.BlockFile.Writer;

public class HdfsDataArchiveManagerTest implements TestExtras {

  private static final String XML = ".xml";
  private static final String HADOOP_CONFIG_PATH = "HADOOP_CONFIG_PATH";
  private static EmbeddedHdfsCluster _embeddedHdfsCluster;
  private static ExecutorService _executorService;
  private static DelayedResourceCleanup _delayedResourceCleanup = new DelayedResourceCleanup(TimeUnit.SECONDS, 10);

  @BeforeClass
  public static void setup() throws IOException {
    if (runEmbeddedTests()) {
      _embeddedHdfsCluster = new EmbeddedHdfsCluster();
      _embeddedHdfsCluster.startup();
    }
    _executorService = Executors.newCachedThreadPool();
  }

  @AfterClass
  public static void teardown() {
    if (runEmbeddedTests()) {
      if (_embeddedHdfsCluster != null) {
        _embeddedHdfsCluster.shutdown();
      }
    }
    _executorService.shutdownNow();
  }

  @Test
  public void testHdfsDataArchiveManager() throws IOException, InterruptedException, ExecutionException {
    Configuration config = getConfiguration();
    Path root = new Path("/tmp/testHdfsDataArchiveManager");
    FileSystem fileSystem = root.getFileSystem(config);
    fileSystem.delete(root, true);
    fileSystem.mkdirs(root);
    int blockSize = 1024;
    String testName = "testHdfsDataArchiveManager";
    PackStorageMetaData metaData = getMetaData(blockSize, testName);
    try (HdfsDataArchiveManager archiveManager = new HdfsDataArchiveManager(_delayedResourceCleanup, metaData, config,
        root, UserGroupInformation.getCurrentUser())) {

      Random random = new Random();
      writeNewBlock(root, fileSystem, blockSize, random, 0);
      archiveManager.updateBlocks();
      assertEquals(1, archiveManager.getBlockReader()
                                    .getLeaves()
                                    .size());

      writeNewBlock(root, fileSystem, blockSize, random, 100);
      archiveManager.updateBlocks();
      assertEquals(2, archiveManager.getBlockReader()
                                    .getLeaves()
                                    .size());

      writeNewBlock(root, fileSystem, blockSize, random, 200);
      archiveManager.updateBlocks();
      assertEquals(3, archiveManager.getBlockReader()
                                    .getLeaves()
                                    .size());
    }

  }

  private void writeNewBlock(Path root, FileSystem fileSystem, int blockSize, Random random, long offset)
      throws IOException {
    Path blockPath = new Path(root, UUID.randomUUID()
                                        .toString());
    try (Writer writer = BlockFile.create(true, fileSystem, blockPath, blockSize, () -> {
      if (!fileSystem.rename(blockPath, new Path(root, offset + ".block"))) {
        throw new IOException("Commit of " + blockPath + " failed.");
      }
    })) {
      for (long longKey = 0; longKey < 100; longKey++) {
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);
        BytesWritable value = new BytesWritable(bytes);
        writer.append(longKey, value);
      }
    }
  }

  private Configuration getConfiguration() throws IOException {
    if (runEmbeddedTests()) {
      return _embeddedHdfsCluster.getFileSystem()
                                 .getConf();
    } else {
      Configuration configuration = new Configuration();
      String path = System.getenv(HADOOP_CONFIG_PATH);
      File file = new File(path);
      if (file.exists() && file.isDirectory()) {
        for (File f : file.listFiles((FilenameFilter) (dir, name) -> name.endsWith(XML))) {
          configuration.addResource(new FileInputStream(f));
        }
      }
      return configuration;
    }
  }

  private static boolean runEmbeddedTests() {
    return System.getenv(HADOOP_CONFIG_PATH) == null;
  }
}
