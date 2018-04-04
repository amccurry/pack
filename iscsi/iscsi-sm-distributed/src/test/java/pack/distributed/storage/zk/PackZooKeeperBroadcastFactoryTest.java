package pack.distributed.storage.zk;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.Md5Utils;

import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.broadcast.PackBroadcastReader;
import pack.distributed.storage.broadcast.PackBroadcastWriter;
import pack.distributed.storage.hdfs.MaxBlockLayer;
import pack.distributed.storage.minicluster.EmbeddedZookeeper;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.read.BlockReader;
import pack.distributed.storage.status.BlockUpdateInfoBatch;
import pack.distributed.storage.status.ServerStatusManager;
import pack.distributed.storage.trace.PackTracer;
import pack.distributed.storage.wal.WalCacheManager;

public class PackZooKeeperBroadcastFactoryTest {

  private static Logger LOGGER = LoggerFactory.getLogger(PackZooKeeperBroadcastFactoryTest.class);

  private static EmbeddedZookeeper ZOOKEEPER;

  @BeforeClass
  public static void setupClass() throws IOException {
    ZOOKEEPER = new EmbeddedZookeeper();
    ZOOKEEPER.startup();
  }

  @AfterClass
  public static void teardownClass() {
    ZOOKEEPER.shutdown();
  }

  @Test
  public void testPackZooKeeperBroadcastFactory() throws IOException, InterruptedException, ConfigException {
    String connection = ZOOKEEPER.getConnection();
    Random random = new Random();
    try (PackTracer tracer = PackTracer.create(LOGGER, "test")) {
      try (ZooKeeperClient zk = ZkUtils.newZooKeeper(connection, 30000)) {
        PackZooKeeperBroadcastFactory factory = new PackZooKeeperBroadcastFactory(zk);

        PackMetaData metaData = PackMetaData.builder()
                                            .build();
        WriteBlockMonitor writeBlockMonitor = getWriteBlockMonitor();
        ServerStatusManager serverStatusManager = getServerStatusManager();
        try (PackBroadcastWriter writer = factory.createPackBroadcastWriter("test", metaData, writeBlockMonitor,
            serverStatusManager)) {

          int blockId = random.nextInt(Integer.MAX_VALUE);
          byte[] bs = new byte[4096];
          random.nextBytes(bs);

          AtomicBoolean test = new AtomicBoolean();

          WalCacheManager walCacheManager = getWalCacheManager(blockId, bs, test);
          MaxBlockLayer maxBlockLayer = getMaxBlockLayer();
          try (PackBroadcastReader reader = factory.createPackBroadcastReader("test", metaData, walCacheManager,
              maxBlockLayer)) {
            reader.start();

            writer.write(tracer, blockId, bs, 0, bs.length);
            writer.flush(tracer);
            Thread.sleep(1000);
            assertTrue(test.get());
          }
        }
      }
    }
  }

  private MaxBlockLayer getMaxBlockLayer() {
    return new MaxBlockLayer() {
      @Override
      public long getMaxLayer() {
        return 0;
      }
    };
  }

  private WalCacheManager getWalCacheManager(int bid, byte[] data, AtomicBoolean test) {
    return new WalCacheManager() {

      @Override
      public void close() throws IOException {

      }

      @Override
      public void writeWalCacheToHdfs() throws IOException {

      }

      @Override
      public void write(long transId, long offset, int blockId, byte[] bs) throws IOException {
        System.out.printf("t:%20d o:%20d b:%20d d:%s%n", transId, offset, blockId, Md5Utils.md5AsBase64(bs));
        System.out.printf("t:%20d o:%20d b:%20d d:%s%n", 0, 0, bid, Md5Utils.md5AsBase64(data));
        if (blockId == bid && Arrays.equals(bs, data)) {
          test.set(true);
        }
      }

      @Override
      public void removeOldWalCache() throws IOException {

      }

      @Override
      public long getMaxLayer() {
        return 0;
      }

      @Override
      public BlockReader getBlockReader() throws IOException {
        return null;
      }
    };
  }

  private WriteBlockMonitor getWriteBlockMonitor() {
    return new WriteBlockMonitor() {
    };
  }

  private ServerStatusManager getServerStatusManager() {
    return new ServerStatusManager() {

      @Override
      public void close() throws IOException {

      }

      @Override
      public void register(String name, WriteBlockMonitor monitor) {

      }

      @Override
      public boolean isLeader(String name) {
        return false;
      }

      @Override
      public void broadcastToAllServers(BlockUpdateInfoBatch updateBlockIdBatch) {

      }
    };
  }

}
