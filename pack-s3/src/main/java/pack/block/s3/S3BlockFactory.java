package pack.block.s3;

import java.io.File;
import java.util.UUID;

import com.amazonaws.services.s3.AmazonS3;

import pack.block.Block;
import pack.block.BlockConfig;
import pack.block.BlockFactory;

public class S3BlockFactory implements BlockFactory {

  private final String _bucketName;
  private final AmazonS3 _client;
  private final File _cacheDir;
  private final String _prefix;

  public S3BlockFactory(S3BlockFactoryConfig config) {
    _bucketName = config.getBucketName();
    _client = config.getClient();
    _cacheDir = config.getCacheDir();
    _prefix = config.getPrefix();
  }

  @Override
  public Block createBlock(BlockConfig config) throws Exception {
    File localCacheFile = getLocalFileCache(config.getVolume(), config.getBlockId());
    return new S3Block(S3BlockConfig.builder()
                                    .blockConfig(config)
                                    .bucketName(_bucketName)
                                    .client(_client)
                                    .localCacheFile(localCacheFile)
                                    .prefix(_prefix)
                                    .build());
  }

  private File getLocalFileCache(String volume, long blockId) {
    File volumeDir = new File(_cacheDir, volume);
    volumeDir.mkdirs();
    return new File(volumeDir, Long.toString(blockId) + "-" + UUID.randomUUID()
                                                                  .toString());
  }

}
