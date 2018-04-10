package pack.distributed.storage.walcache;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import com.amazonaws.util.Md5Utils;

import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.HdfsBlockGarbageCollector;
import pack.distributed.storage.hdfs.HdfsMiniClusterUtil;
import pack.distributed.storage.hdfs.PackHdfsReader;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.read.BlockReader;
import pack.distributed.storage.read.ReadRequest;
import pack.distributed.storage.status.BroadcastServerManager;
import pack.distributed.storage.walcache.PackWalCache;
import pack.distributed.storage.walcache.PackWalCacheFactory;
import pack.distributed.storage.walcache.PackWalCacheManager;
import pack.distributed.storage.walcache.WalCacheFactory;
import pack.distributed.storage.status.BlockUpdateInfoBatch;
import pack.iscsi.storage.utils.PackUtils;

public class PackWalCacheManagerTest {

  private static final File TMPDIR = new File(
      System.getProperty("hdfs.tmp.dir", "./target/tmp/PackWalCacheManagerTest/hdfs"));

  private static MiniDFSCluster _cluster;

  @BeforeClass
  public static void startCluster() {
    _cluster = HdfsMiniClusterUtil.startDfs(new Configuration(), true, TMPDIR.getAbsolutePath());
  }

  @AfterClass
  public static void stopCluster() {
    HdfsMiniClusterUtil.shutdownDfs(_cluster);
  }

  private File _dirFile;

  @Before
  public void setup() {
    _dirFile = new File("./target/tmp/PackWalCacheManagerTest/cache");
    PackUtils.rmr(_dirFile);
    _dirFile.mkdirs();
  }

  @Test
  public void testPackWalCacheManager() throws IOException {
    Random random = new Random(1);
    int blockSize = 1000;
    int maxNumberOfBlocks = 10000;
    int maxNumberOfBlocksToWrite = 10;
    int rollInterval = 11;
    long length = (long) maxNumberOfBlocks * (long) blockSize;
    PackMetaData metaData = PackMetaData.builder()
                                        .blockSize(blockSize)
                                        .length(length)
                                        .build();

    String volumeName = "testPackWalCacheManager";
    Path volumeDir = new Path("/testPackWalCacheManager");
    Configuration configuration = _cluster.getFileSystem()
                                          .getConf();
    try (PackHdfsReader hdfsReader = new PackHdfsReader(configuration, volumeDir, UserGroupInformation.getCurrentUser(),
        getHdfsBlockGC())) {
      WalCacheFactory cacheFactory = new PackWalCacheFactory(metaData, _dirFile);
      BroadcastServerManager ssm = newServerStatusManager();
      try (PackWalCacheManager manager = new PackWalCacheManager(volumeName, WriteBlockMonitor.NO_OP, cacheFactory,
          hdfsReader, ssm, metaData, configuration, volumeDir, 1_000_000, TimeUnit.SECONDS.toMillis(10), false)) {
        File file = new File("./target/tmp/PackWalCacheManagerTest/test");
        byte[] buffer = new byte[blockSize];
        long layer = 0;
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        try (RandomAccessFile rand = new RandomAccessFile(file, "rw")) {
          for (int i = 0; i < maxNumberOfBlocksToWrite; i++) {
            if (i % rollInterval == 0 && i != 0) {
              manager.forceRollOnNextWrite();
            }
            int blockId = random.nextInt(maxNumberOfBlocks);
            roaringBitmap.add(blockId);
            random.nextBytes(buffer);

            System.out.println(blockId + " write " + Md5Utils.md5AsBase64(buffer));

            long pos = (long) blockId * (long) blockSize;
            rand.seek(pos);
            rand.write(buffer, 0, blockSize);
            manager.write(-1L, layer, blockId, buffer);
            layer++;
          }
          for (Integer blockId : roaringBitmap) {
            long pos = (long) blockId * (long) blockSize;
            rand.seek(pos);
            byte[] buf = new byte[blockSize];
            rand.readFully(buf, 0, blockSize);

            byte[] buf2 = new byte[blockSize];
            ByteBuffer dest = ByteBuffer.wrap(buf2);
            try (BlockReader blockReader = manager.getBlockReader()) {
              List<ReadRequest> list = Arrays.asList(new ReadRequest(blockId, 0, dest));
              boolean readBlocks = blockReader.readBlocks(list);
              assertFalse(readBlocks);
            }

            System.out.println(blockId + " const " + Md5Utils.md5AsBase64(buf));
            System.out.println(blockId + " wal   " + Md5Utils.md5AsBase64(buf2) + " " + Arrays.toString(buf2));

            assertTrue(Arrays.equals(buf, buf2));
          }

          manager.writeWalCacheToHdfs();

          long maxLayer = hdfsReader.getMaxLayer();

          try (BlockReader reader = manager.getBlockReader()) {
            List<BlockReader> leaves = reader.getLeaves();
            for (BlockReader blockReader : leaves) {
              PackWalCache cache = (PackWalCache) blockReader;
              long cachemaxLayer = cache.getMaxLayer();
              assertTrue(maxLayer + "<" + cachemaxLayer, maxLayer < cachemaxLayer);
            }
          }
        }
      }
    } finally {
    }
  }

  private HdfsBlockGarbageCollector getHdfsBlockGC() {
    return new HdfsBlockGarbageCollector() {
      @Override
      public void add(Path path) {

      }

      @Override
      public void close() throws IOException {

      }
    };
  }

  private BroadcastServerManager newServerStatusManager() {
    return new BroadcastServerManager() {

      @Override
      public void register(String name, WriteBlockMonitor monitor) {

      }

      @Override
      public boolean isLeader(String name) {
        return true;
      }

      @Override
      public void broadcastToAllServers(BlockUpdateInfoBatch updateBlockIdBatch) {

      }

      @Override
      public void close() throws IOException {

      }
    };
  }
}
