package pack.s3;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import pack.block.BlockFactory;
import pack.block.BlockManagerConfig;
import pack.block.CrcBlockManager;
import pack.block.s3.S3BlockFactory;
import pack.block.s3.S3BlockFactoryConfig;
import pack.block.zk.ZkCrcBlockManager;
import pack.block.zk.ZkCrcBlockManagerConfig;

public class FileHandleManger {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileHandleManger.class);

  private final String _cacheLocation;
  private final long _blockSize = 64 * 1024 * 1024;
  private final BlockingQueue<byte[]> _queue;
  private String _bucketName;
  private final CuratorFramework _client;
  private final AmazonS3 _s3Client;

  public FileHandleManger(String cacheLocation, String syncLocations, String zk) throws InterruptedException {
    _cacheLocation = cacheLocation;
    int capacity = 100;
    _queue = new ArrayBlockingQueue<>(capacity + 1);
    for (int i = 0; i < capacity; i++) {
      _queue.put(new byte[128 * 1024]);
    }
    RetryPolicy retryPolicy = new RetryForever((int) TimeUnit.SECONDS.toMillis(10));
    _client = CuratorFrameworkFactory.newClient(zk, retryPolicy);
    _client.getUnhandledErrorListenable()
           .addListener((message, e) -> {
             LOGGER.error("Unknown error " + message, e);
           });
    _client.getConnectionStateListenable()
           .addListener((c, newState) -> {
             LOGGER.info("Connection state {}", newState);
           });
    _client.start();
    _s3Client = AmazonS3ClientBuilder.defaultClient();
  }

  public List<String> getVolumes() {
    return Arrays.asList("testv2");
  }

  public long getVolumeSize(String volumeName) {
    return 100L * 1024L * 1024L * 1024L;
  }

  public FileHandle createHandle(String volumeName) throws Exception {
    BlockFactory blockFactory = getBlockFactory();
    CrcBlockManager crcBlockManager = getCrcBlockManager(volumeName);
    long blockSize = _blockSize;
    long cacheSize = _blockSize * 1000;
    BlockManagerConfig config = BlockManagerConfig.builder()
                                                  .blockFactory(blockFactory)
                                                  .blockSize(blockSize)
                                                  .cacheSize(cacheSize)
                                                  .crcBlockManager(crcBlockManager)
                                                  .volume(volumeName)
                                                  .build();
    return new BlockManagerFileHandle(_queue, config);
  }

  private BlockFactory getBlockFactory() {
    return new S3BlockFactory(S3BlockFactoryConfig.builder()
                                                  .bucketName(_bucketName)
                                                  .cacheDir(new File(_cacheLocation))
                                                  .client(_s3Client)
                                                  .build());
  }

  private CrcBlockManager getCrcBlockManager(String volumeName) {
    return new ZkCrcBlockManager(ZkCrcBlockManagerConfig.builder()
                                                        .volume(volumeName)
                                                        .client(_client)
                                                        .build());
  }

}
