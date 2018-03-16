package pack.distributed.storage;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.jscsi.target.Target;

import com.google.common.collect.ImmutableList;

import pack.distributed.storage.kafka.PackKafkaClientFactory;
import pack.iscsi.storage.utils.PackUtils;

public class LocalTest {

  public static void main(String[] args) throws Exception {
    Random random = new Random(1);
    String name = "test";

    Configuration configuration = PackConfig.getConfiguration();
    UserGroupInformation ugi = PackConfig.getUgi();
    Path volume = new Path("/tmp/testpack/" + name);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {

      FileSystem fileSystem = volume.getFileSystem(configuration);

      PackMetaData metaData = PackMetaData.read(configuration, volume);

      String kafkaZkConnection = PackConfig.getKafkaZkConnection();
      PackKafkaClientFactory clientFactory = new PackKafkaClientFactory(kafkaZkConnection);
      String newTopicId = PackUtils.getTopic(name, UUID.randomUUID()
                                                       .toString());
      try (AdminClient admin = clientFactory.createAdmin()) {
        if (metaData != null) {
          admin.deleteTopics(ImmutableList.of(metaData.getTopicId()));
        }
        admin.createTopics(ImmutableList.of(new NewTopic(newTopicId, 1, (short) 3)));
        Thread.sleep(3000);
      }

      fileSystem.delete(new Path("/tmp/testpack"), true);
      fileSystem.mkdirs(volume);
      PackMetaData.builder()
                  .length(100_000_000_000L)
                  .blockSize(4096)
                  .topicId(newTopicId)
                  .build()
                  .write(configuration, volume);
      return null;
    });

    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      PackStorageTargetManagerFactory factory = new PackStorageTargetManagerFactory();
      PackStorageTargetManager manager = (PackStorageTargetManager) factory.create(null);
      manager.getTargetNames();
      String fullName = manager.getFullName("test");
      Target target = manager.getTarget(fullName);
      PackStorageModule storageModule = (PackStorageModule) target.getStorageModule();
      int blockSize = storageModule.getBlockSize();

      int maxNumberOfBlocks = (int) storageModule.getSizeInBlocks();
      // if (testWrites) {
      // testWrites(random, storageModule, blockSize);
      // } else {
      // testReads(random, storageModule, blockSize);
      // }
      while (true) {
        testBoth(random, storageModule, blockSize, maxNumberOfBlocks);
      }
      // return null;
    });
  }

  private static void testBoth(Random random, PackStorageModule storageModule, int blockSize, int maxNumberOfBlocks)
      throws IOException, InterruptedException, NoSuchAlgorithmException {
    byte[] buf = new byte[blockSize];
    byte[] readBuf = new byte[blockSize];
    MessageDigest messageDigest = MessageDigest.getInstance("md5");
    for (int i = 0; i < 1000; i++) {
      random.nextBytes(buf);
      int blockId = random.nextInt(maxNumberOfBlocks);
      long storageIndex = PackUtils.getPosition(blockId, blockSize);
      storageModule.write(buf, storageIndex);
      storageModule.flushWrites();
      storageModule.read(readBuf, storageIndex);
      if (!Arrays.equals(buf, readBuf)) {

        System.out.println(new BigInteger(messageDigest.digest(readBuf)));
        System.out.println(new BigInteger(messageDigest.digest(buf)));

        throw new RuntimeException("block " + blockId + " not equal");
      }
    }
  }

  // private static void testReads(Random random, PackStorageModule
  // storageModule, int blockSize) throws IOException {
  // byte[] buf = new byte[blockSize];
  // byte[] readBuf = new byte[blockSize];
  // for (int i = 0; i < 1000; i++) {
  // random.nextBytes(buf);
  // long storageIndex = i * blockSize;
  // storageModule.read(readBuf, storageIndex);
  // if (!Arrays.equals(buf, readBuf)) {
  // throw new RuntimeException("block " + i + " not equal");
  // }
  // }
  // }
  //
  // private static void testWrites(Random random, PackStorageModule
  // storageModule, int blockSize) throws IOException {
  // byte[] buf = new byte[blockSize];
  // for (int i = 0; i < 1000; i++) {
  // random.nextBytes(buf);
  // long storageIndex = i * blockSize;
  // storageModule.write(buf, storageIndex);
  // }
  // storageModule.flushWrites();
  // }

}
