package pack.distributed.storage.wal;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.Md5Utils;
import com.google.common.io.Closer;

import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.MaxBlockLayer;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.read.BlockReader;
import pack.distributed.storage.status.BlockUpdateInfoBatch;
import pack.distributed.storage.status.BroadcastServerManager;
import pack.distributed.storage.trace.PackTracer;
import pack.distributed.storage.walcache.WalCacheManager;

public abstract class PackWalFactoryTestBase {

  private static Logger LOGGER = LoggerFactory.getLogger(PackWalFactoryTestBase.class);
  protected Closer _closer;

  @Before
  public void setup() {
    _closer = Closer.create();
  }

  @After
  public void teardown() throws IOException {
    _closer.close();
  }

  @Test
  public void testPackWalFactory() throws Exception {
    Random random = new Random();
    Object lock = new Object();
    try (PackTracer tracer = PackTracer.create(LOGGER, "test")) {
      try (PackWalFactory factory = createPackWalFactory()) {

        PackMetaData metaData = PackMetaData.builder()
                                            .build();
        WriteBlockMonitor writeBlockMonitor = getWriteBlockMonitor();
        BroadcastServerManager serverStatusManager = getServerStatusManager();
        try (PackWalWriter writer = factory.createPackWalWriter("test", metaData, writeBlockMonitor,
            serverStatusManager)) {

          int blockId = random.nextInt(Integer.MAX_VALUE);
          byte[] bs = new byte[4096];
          random.nextBytes(bs);

          AtomicBoolean test = new AtomicBoolean();

          WalCacheManager walCacheManager = getWalCacheManager(blockId, bs, test, lock);
          MaxBlockLayer maxBlockLayer = getMaxBlockLayer();
          try (PackWalReader reader = factory.createPackWalReader("test", metaData, walCacheManager, maxBlockLayer)) {
            reader.start();
            synchronized (lock) {
              writer.write(tracer, blockId, bs, 0, bs.length);
              writer.flush(tracer);
              lock.wait(TimeUnit.MINUTES.toMillis(1));
            }
            assertTrue(test.get());
          }
        }
      }
    }
  }

  protected abstract PackWalFactory createPackWalFactory() throws Exception;

  private MaxBlockLayer getMaxBlockLayer() {
    return new MaxBlockLayer() {
      @Override
      public long getMaxLayer() {
        return 0;
      }
    };
  }

  private WalCacheManager getWalCacheManager(int bid, byte[] data, AtomicBoolean test, Object lock) {
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
        synchronized (lock) {
          lock.notify();
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

  private BroadcastServerManager getServerStatusManager() {
    return new BroadcastServerManager() {

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
