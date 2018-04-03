package pack.distributed.storage;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.jscsi.target.Target;
import org.jscsi.target.storage.IStorageModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Closer;

import pack.distributed.storage.minicluster.EmbeddedHdfsCluster;
import pack.distributed.storage.minicluster.EmbeddedZookeeper;
import pack.distributed.storage.minicluster.IscsiMiniCluster;
import pack.iscsi.storage.utils.PackUtils;

public class PackStorageModuleTest {

  private static EmbeddedZookeeper zookeeper;
  private static EmbeddedHdfsCluster hdfsCluster;

  @BeforeClass
  public static void setup() throws IOException {
    zookeeper = new EmbeddedZookeeper();
    zookeeper.startup();

    hdfsCluster = new EmbeddedHdfsCluster();
    hdfsCluster.startup();

    File hadoopConf = new File("./target/tmp/PackStorageModuleTest/hdfs-conf");
    IscsiMiniCluster.writeConfig(hdfsCluster.getFileSystem()
                                            .getConf(),
        hadoopConf);

    System.setProperty("WAL_CACHE_DIR", "./target/tmp/PackStorageModuleTest/wal");
    System.setProperty("HDFS_CONF_PATH", hadoopConf.getAbsolutePath());
    System.setProperty("HDFS_TARGET_PATH", "/pack");
    System.setProperty("ZK_CONNECTION", zookeeper.getConnection());
    System.setProperty("PACK_ISCSI_ADDRESS", InetAddress.getLocalHost()
                                                        .getHostAddress());
  }

  @AfterClass
  public static void teardown() {
    hdfsCluster.shutdown();
    zookeeper.shutdown();
  }

  @Test
  public void testPackStorageModule() throws IOException {

    Path volume = new Path("/pack/test");
    Configuration conf = hdfsCluster.getFileSystem()
                                    .getConf();
    String serialId = UUID.randomUUID()
                          .toString();
    long length = 10_000_000_000L;
    int blockSize = 4096;
    PackMetaData.builder()
                .length(length)
                .blockSize(blockSize)
                .serialId(serialId)
                .build()
                .write(conf, volume);

    Random random = new Random(1);

    try (PackStorageTargetManager targetManager = new PackStorageTargetManager()) {
      targetManager.getTargetNames();
      Target target = targetManager.getTarget(targetManager.getFullName("test"));
      try (IStorageModule storageModule = target.getStorageModule()) {
        for (int i = 0; i < 100000; i++) {
          int blockId = random.nextInt((int) (length / blockSize));
          long position = PackUtils.getPosition(blockId, blockSize);

          byte[] writeBuf = new byte[blockSize];
          random.nextBytes(writeBuf);
          storageModule.write(writeBuf, position);

          storageModule.flushWrites();

          byte[] readbuf = new byte[blockSize];
          storageModule.read(readbuf, position);

          if (!Arrays.equals(writeBuf, readbuf)) {
            System.out.println(Arrays.toString(readbuf));
            System.out.println(Arrays.toString(writeBuf));
            assertTrue(i + " Block " + blockId, false);
          }
        }
      }
    }
  }

  @Test
  public void testPackStorageModuleMulti() throws IOException, InterruptedException {

    Path volume = new Path("/pack/test");
    Configuration conf = hdfsCluster.getFileSystem()
                                    .getConf();
    String serialId = UUID.randomUUID()
                          .toString();
    long length = 10_000_000_000L;
    int blockSize = 4096;
    PackMetaData.builder()
                .length(length)
                .blockSize(blockSize)
                .serialId(serialId)
                .build()
                .write(conf, volume);

    Random random = new Random(1);

    try (Closer closer = Closer.create()) {
      System.setProperty(PackConfig.WRITE_BLOCK_MONITOR_ADDRESS, "127.0.0.10");
      System.setProperty(PackConfig.PACK_ISCSI_ADDRESS, "127.0.0.10");
      System.setProperty(PackConfig.PACK_HTTP_ADDRESS, "127.0.0.10");
      PackStorageTargetManager targetManager1 = closer.register(new PackStorageTargetManager("127.0.0.10"));

      System.setProperty(PackConfig.WRITE_BLOCK_MONITOR_ADDRESS, "127.0.0.11");
      System.setProperty(PackConfig.PACK_ISCSI_ADDRESS, "127.0.0.11");
      System.setProperty(PackConfig.PACK_HTTP_ADDRESS, "127.0.0.11");
      PackStorageTargetManager targetManager2 = closer.register(new PackStorageTargetManager("127.0.0.11"));

      targetManager1.getTargetNames();
      Target target1 = targetManager1.getTarget(targetManager1.getFullName("test"));

      targetManager2.getTargetNames();
      Target target2 = targetManager2.getTarget(targetManager2.getFullName("test"));

      IStorageModule storageModule1 = closer.register(target1.getStorageModule());
      IStorageModule storageModule2 = closer.register(target2.getStorageModule());

      for (int i = 0; i < 1000; i++) {
        int blockId = random.nextInt((int) (length / blockSize));
        long position = PackUtils.getPosition(blockId, blockSize);
        byte[] writeBuf = new byte[blockSize];
        random.nextBytes(writeBuf);
        {
          IStorageModule storageModule = chooseOne(random, storageModule1, storageModule2);
          storageModule.write(writeBuf, position);
          storageModule.flushWrites();
          Thread.sleep(10);
        }
        readCheck(blockSize, i, blockId, position, writeBuf, storageModule1);
        readCheck(blockSize, i, blockId, position, writeBuf, storageModule2);
      }
    }
  }

  private void readCheck(int blockSize, int i, int blockId, long position, byte[] writeBuf,
      IStorageModule storageModule) throws IOException {
    byte[] readbuf = new byte[blockSize];
    storageModule.read(readbuf, position);
    if (!Arrays.equals(writeBuf, readbuf)) {
      System.out.println(Arrays.toString(readbuf));
      System.out.println(Arrays.toString(writeBuf));
      assertTrue(i + " Block " + blockId, false);
    }
  }

  private IStorageModule chooseOne(Random random, IStorageModule... storageModules) {
    int index = random.nextInt(storageModules.length);
    return storageModules[index];
  }

}
