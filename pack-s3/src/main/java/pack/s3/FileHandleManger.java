package pack.s3;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

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

  private final String _cacheLocation;
  private final long _blockSize = 64 * 1024 * 1024;
  private BlockingQueue<byte[]> _queue;

  public FileHandleManger(String cacheLocation, String syncLocations) throws InterruptedException {
    _cacheLocation = cacheLocation;
    int capacity = 100;
    _queue = new ArrayBlockingQueue<>(capacity + 1);
    for (int i = 0; i < capacity; i++) {
      _queue.put(new byte[128 * 1024]);
    }
  }

  public List<String> getVolumes() {
    return Arrays.asList("testv2");
  }

  public long getVolumeSize(String volumeName) {
    return 100L * 1024L * 1024L * 1024L;
  }

  public FileHandle createHandle(String volumeName) throws Exception {
    String bucketName;
    File cacheDir = new File(_cacheLocation);
    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    S3BlockFactoryConfig s3BlockFactoryConfig = S3BlockFactoryConfig.builder()
                                                                    .bucketName(bucketName)
                                                                    .cacheDir(cacheDir)
                                                                    .client(client)
                                                                    .build();
    BlockFactory blockFactory = new S3BlockFactory(s3BlockFactoryConfig);
    ZkCrcBlockManagerConfig zkCrcBlockManagerConfig = ZkCrcBlockManagerConfig.builder()
                                                                             .volume(volumeName)
                                                                             .zk(zk)
                                                                             .build();
    CrcBlockManager crcBlockManager = new ZkCrcBlockManager(zkCrcBlockManagerConfig);
    long blockSize = _blockSize;
    long cacheSize = _blockSize * 1000;
    long crcBlockSize = _blockSize;
    BlockManagerConfig config = BlockManagerConfig.builder()
                                                  .blockFactory(blockFactory)
                                                  .blockSize(blockSize)
                                                  .cacheSize(cacheSize)
                                                  .crcBlockManager(crcBlockManager)
                                                  .crcBlockSize(crcBlockSize)
                                                  .volume(volumeName)
                                                  .build();
    return new BlockManagerFileHandle(_queue, config);
  }

}
