package pack.s3;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.ImmutableMap;

import consistent.s3.ConsistentAmazonS3;
import consistent.s3.ConsistentAmazonS3Config;
import pack.block.BlockFactory;
import pack.block.BlockManagerConfig;
import pack.block.CrcBlockManager;
import pack.block.s3.S3BlockFactory;
import pack.block.s3.S3BlockFactoryConfig;
import pack.block.s3.S3CrcBlockManager;
import pack.block.s3.S3CrcBlockManagerConfig;
import pack.block.s3.S3MetadataStore;
import pack.block.s3.S3MetadataStoreConfig;
import pack.block.zk.ZkCrcBlockManager;
import pack.block.zk.ZkCrcBlockManagerConfig;

public class FileHandleManger {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileHandleManger.class);

  private static final String S3 = "/s3";
  private final File _cacheDir;
  private final long _blockSize = 64 * 1024 * 1024;
  private final BlockingQueue<byte[]> _queue;
  private final String _bucketName;
  private final CuratorFramework _client;
  private final AmazonS3 _s3Client;
  private final String _prefix = "";
  private final MetadataStore _metadataStore;
  private final Map<String, FileHandle> _handles = new ConcurrentHashMap<>();
  private final File _uploadDir;
  private final ConsistentAmazonS3 _consistentS3Client;

  public FileHandleManger(String localRoot, String syncLocations, String zk, String bucketName) throws Exception {
    _bucketName = bucketName;
    File root = new File(localRoot);
    _cacheDir = new File(root, "cache");
    _cacheDir.mkdirs();
    _uploadDir = new File(root, "upload");
    _uploadDir.mkdirs();

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

    _consistentS3Client = ConsistentAmazonS3.create(_s3Client, _client, ConsistentAmazonS3Config.DEFAULT.toBuilder()
                                                                                                        .zkPrefix(S3)
                                                                                                        .build());

    S3MetadataStoreConfig config = S3MetadataStoreConfig.builder()
                                                        .bucketName(_bucketName)
                                                        .prefix(_prefix)
                                                        .client(_s3Client)
                                                        .consistentS3Client(_consistentS3Client)
                                                        .build();
    _metadataStore = new S3MetadataStore(config);
  }

  public List<String> getVolumes() throws Exception {
    return _metadataStore.getVolumes();
  }

  public long getVolumeSize(String volumeName) throws Exception {
    return _metadataStore.getVolumeSize(volumeName);
  }

  public void setVolumeSize(String volumeName, long length) throws Exception {
    _metadataStore.setVolumeSize(volumeName, length);
  }

  public synchronized FileHandle getHandle(String volumeName) throws Exception {
    FileHandle fileHandle = _handles.get(volumeName);
    if (fileHandle != null) {
      fileHandle.incRef();
      return fileHandle;
    }
    return createHandle(volumeName);
  }

  private FileHandle createHandle(String volumeName) throws Exception {
    if (_metadataStore.isMounted(volumeName)) {
      throw new IOException("Volume is already mounted");
    }
    _metadataStore.mount(volumeName);
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

    FileHandle fileHandle = new BlockManagerFileHandle(_queue, config, () -> {
      _metadataStore.umount(volumeName);
      _handles.remove(volumeName);
    });
    fileHandle.incRef();
    _handles.put(volumeName, fileHandle);
    return fileHandle;
  }

  private BlockFactory getBlockFactory() {
    return new S3BlockFactory(S3BlockFactoryConfig.builder()
                                                  .bucketName(_bucketName)
                                                  .cacheDir(_cacheDir)
                                                  .client(_s3Client)
                                                  .prefix(_prefix)
                                                  .uploadDir(_uploadDir)
                                                  .build());
  }

  private CrcBlockManager getCrcBlockManager(String volumeName) throws Exception {
    ZkCrcBlockManager baseCrcBlockManager = new ZkCrcBlockManager(ZkCrcBlockManagerConfig.builder()
                                                                                         .volume(volumeName)
                                                                                         .client(_client)
                                                                                         .build());

    S3CrcBlockManagerConfig s3CrcBlockManagerConfig = S3CrcBlockManagerConfig.builder()
                                                                             .bucketName(_bucketName)
                                                                             .client(_s3Client)
                                                                             .crcBlockManager(baseCrcBlockManager)
                                                                             .prefix(_prefix)
                                                                             .volume(volumeName)
                                                                             .build();
    return new S3CrcBlockManager(s3CrcBlockManagerConfig);
  }

  public Map<String, FileHandle> getCurrentHandles() {
    return ImmutableMap.copyOf(_handles);
  }

}
