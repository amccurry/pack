package pack.iscsi.storage;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.kafka.EmbeddedKafkaCluster;
import pack.iscsi.kafka.EmbeddedZookeeper;
import pack.iscsi.storage.kafka.PackKafkaManager;
import pack.iscsi.storage.utils.IOUtils;

public class DataSyncManagerTest implements TestExtras {

  private static final String KAFKA_BROKER_LIST = "KAFKA_BROKER_LIST";

  private final static Logger LOGGER = LoggerFactory.getLogger(DataSyncManagerTest.class);

  private static EmbeddedZookeeper _embeddedZookeeper;
  private static EmbeddedKafkaCluster _embeddedKafkaCluster;
  private static DelayedResourceCleanup _delayedResourceCleanup = new DelayedResourceCleanup(TimeUnit.SECONDS, 10);

  @BeforeClass
  public static void setup() throws IOException {
    File file = new File("./target/test");
    IOUtils.rmr(file);
    if (runEmbeddedTests()) {
      _embeddedZookeeper = new EmbeddedZookeeper();
      _embeddedZookeeper.startup();
      _embeddedKafkaCluster = new EmbeddedKafkaCluster(_embeddedZookeeper.getConnection(), new Properties(),
          Arrays.asList(-1, -1, -1));
      _embeddedKafkaCluster.startup();
    }

  }

  @AfterClass
  public static void teardown() {
    if (runEmbeddedTests()) {
      if (_embeddedKafkaCluster != null) {
        _embeddedKafkaCluster.shutdown();
        _embeddedKafkaCluster.awaitShutdown();
      }
      if (_embeddedZookeeper != null) {
        _embeddedZookeeper.shutdown();
      }
    }
  }

  private String getBrokerList() {
    if (runEmbeddedTests()) {
      return _embeddedKafkaCluster.getBrokerList();
    } else {
      return System.getenv(KAFKA_BROKER_LIST);
    }
  }

  private static boolean runEmbeddedTests() {
    return System.getenv(KAFKA_BROKER_LIST) == null;
  }

  @Test
  public void testPackDataSyncManagerSimple() throws IOException, InterruptedException, ExecutionException {
    String brokerList = getBrokerList();
    int blockSize = 10;
    int blockId = 8;
    String testName = "testPackDataSyncManagerWalLogRoll";
    PackStorageMetaData metaData = getMetaData(blockSize, testName);

    PackKafkaManager kafkaManager = new PackKafkaManager(brokerList, testName);
    setupTopic(metaData, kafkaManager);

    try (DataSyncManager manager = new DataSyncManager(_delayedResourceCleanup, kafkaManager, metaData)) {
      ByteBuffer dest = ByteBuffer.allocate(blockSize);
      boolean written = false;
      while (true) {
        BlockReader blockReader = manager.getBlockReader();
        manager.checkState();
        ReadRequest request = new ReadRequest(blockId, 0, dest);
        if (blockReader.readBlocks(request)) {
          LOGGER.info("block id not found {}", blockId);
          if (!written) {
            writeBlock(blockId, blockSize, manager, metaData);
            written = true;
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
          } else {
            fail("Block not read in time.");
          }
        } else {
          return;
        }
      }
    }
  }

  @Test
  public void testPackDataSyncManagerWalLogRoll() throws IOException, InterruptedException, ExecutionException {
    String brokerList = getBrokerList();
    int blockSize = 10;
    int blockId = 8;
    String testName = "testPackDataSyncManagerWalLogRoll";
    PackStorageMetaData metaData = getMetaData(blockSize, testName);

    PackKafkaManager kafkaManager = new PackKafkaManager(brokerList, testName);
    setupTopic(metaData, kafkaManager);

    try (DataSyncManager manager = new DataSyncManager(_delayedResourceCleanup, kafkaManager, metaData)) {
      for (int i = 0; i < metaData.getMaxOffsetPerWalFile() + 1; i++) {
        writeBlock(blockId, blockSize, manager, metaData);
      }
      manager.waitForKafkaSyncIfNeeded();
      // Let new wal file get created
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      BlockReader blockReader = manager.getBlockReader();
      List<BlockReader> leaves = blockReader.getLeaves();
      assertEquals(2, leaves.size());
    }
  }

  @Test
  public void testPackDataSyncManagerWalLogCommit() throws IOException, InterruptedException, ExecutionException {
    String brokerList = getBrokerList();
    int blockSize = 10;
    int blockId = 8;
    String testName = "testPackDataSyncManagerWalLogCommit";
    PackStorageMetaData metaData = getMetaData(blockSize, testName);

    PackKafkaManager kafkaManager = new PackKafkaManager(brokerList, testName);
    setupTopic(metaData, kafkaManager);

    try (DataSyncManager manager = new DataSyncManager(_delayedResourceCleanup, kafkaManager, metaData)) {
      for (int i = 0; i < metaData.getMaxOffsetPerWalFile() + 1; i++) {
        writeBlock(blockId, blockSize, manager, metaData);
      }
      manager.waitForKafkaSyncIfNeeded();
      // Let new wal file get created
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      BlockReader blockReader = manager.getBlockReader();
      List<BlockReader> leaves = blockReader.getLeaves();
      assertEquals(2, leaves.size());

      List<WalCache> cache = manager.getPackWalCacheList();
      WalCache packWalCache = cache.get(cache.size() - 1);
      long newCommitOffset = manager.commit(packWalCache.getEndingOffset());
      assertEquals(10, newCommitOffset);
    }
  }

  @Test
  public void testPackDataSyncManagerWalLogRecover() throws IOException, InterruptedException, ExecutionException {
    String brokerList = getBrokerList();
    int blockSize = 10;
    int blockId = 8;
    String testName = "testPackDataSyncManagerWalLogRecover";
    PackStorageMetaData metaData = getMetaData(blockSize, testName);

    PackKafkaManager kafkaManager = new PackKafkaManager(brokerList, testName);
    setupTopic(metaData, kafkaManager);

    try (DataSyncManager manager = new DataSyncManager(_delayedResourceCleanup, kafkaManager, metaData)) {
      for (int i = 0; i < metaData.getMaxOffsetPerWalFile() * 4 + 1; i++) {
        writeBlock(blockId, blockSize, manager, metaData);
      }
      manager.waitForKafkaSyncIfNeeded();
      // Let new wal file get created
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      BlockReader blockReader = manager.getBlockReader();
      List<BlockReader> leaves = blockReader.getLeaves();
      assertEquals(5, leaves.size());

      List<WalCache> cache = manager.getPackWalCacheList();
      WalCache packWalCache = cache.get(cache.size() - 1);
      long newCommitOffset = manager.commit(packWalCache.getEndingOffset());
      assertEquals(10, newCommitOffset);
    }

    IOUtils.rmr(new File("./target/test/" + testName));

    try (DataSyncManager manager = new DataSyncManager(_delayedResourceCleanup, kafkaManager, metaData)) {
      manager.waitForKafkaSyncIfNeeded();
      // Let new wal file get created
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      BlockReader blockReader = manager.getBlockReader();
      List<BlockReader> leaves = blockReader.getLeaves();
      assertEquals(4, leaves.size());
    }
  }

  private void setupTopic(PackStorageMetaData metaData, PackKafkaManager kafkaManager)
      throws InterruptedException, ExecutionException {
    kafkaManager.deleteTopic(metaData.getKafkaTopic());
    kafkaManager.createTopicIfMissing(metaData.getKafkaTopic());
    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
  }

  private void writeBlock(long blockId, int blockSize, DataSyncManager manager, PackStorageMetaData metaData) {
    KafkaProducer<Long, byte[]> kafkaProducer = manager.getKafkaProducer();
    kafkaProducer.send(new ProducerRecord<Long, byte[]>(metaData.getKafkaTopic(), metaData.getKafkaPartition(),
        blockId * blockSize, new byte[blockSize]));
    kafkaProducer.flush();
  }

}
