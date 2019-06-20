package pack.block.s3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import pack.block.Block;
import pack.block.BlockConfig;
import pack.block.CrcBlockManager;
import pack.block.util.CRC64;

public class S3Block implements Block {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3Block.class);

  private static final String RW = "rw";
  private static final Joiner KEY_JOINER = Joiner.on('/');
  private static final String _404_NOT_FOUND = "404 Not Found";
  private static final String BLOCK_CRC = "BLOCK_CRC";

  private final AmazonS3 _client;
  private final CRC64 _crc64;
  private final File _localCacheFile;
  private final RandomAccessFile _random;
  private final FileChannel _channel;
  private final long _blockSize;
  private final AtomicLong _currentCrc = new AtomicLong();
  private final AtomicLong _lastCrcSync = new AtomicLong();
  private final AtomicLong _lastWriteTime = new AtomicLong();
  private final ReentrantLock _writeLock = new ReentrantLock(true);
  private final String _bucketName;
  private final String _baseKey;
  private final CrcBlockManager _crcBlockManager;
  private final long _blockId;
  private final long _consistencyWaitTime;

  public S3Block(S3BlockConfig config) throws Exception {
    BlockConfig blockConfig = config.getBlockConfig();
    _consistencyWaitTime = config.getConsistencyWaitTime();
    _bucketName = config.getBucketName();
    _client = config.getClient();
    _blockId = blockConfig.getBlockId();
    if (config.getPrefix() == null) {
      _baseKey = KEY_JOINER.join(blockConfig.getVolume(), Long.toString(_blockId));
    } else {
      _baseKey = KEY_JOINER.join(config.getPrefix(), blockConfig.getVolume(), Long.toString(_blockId));
    }

    _blockSize = blockConfig.getBlockSize();

    _crcBlockManager = blockConfig.getCrcBlockManager();
    long blockCrc = _crcBlockManager.getBlockCrc(_blockId);

    _currentCrc.set(blockCrc);
    _lastCrcSync.set(blockCrc);
    _crc64 = CRC64.newInstance(blockCrc);
    _localCacheFile = config.getLocalCacheFile();

    load();
    _random = new RandomAccessFile(_localCacheFile, RW);
    _channel = _random.getChannel();
  }

  @Override
  public int read(long position, byte[] buf, int off, int len) throws Exception {
    int length = (int) Math.min(len, _blockSize - position);
    LOGGER.debug("basekey {} read at {} buf len {} off {} len {} length {}", _baseKey, position, buf.length, off, len,
        length);
    return _channel.read(ByteBuffer.wrap(buf, off, length), position);
  }

  @Override
  public int write(long position, byte[] buf, int off, int len) throws Exception {
    _writeLock.lock();
    try {
      int length = (int) Math.min(len, _blockSize - position);
      LOGGER.debug("basekey {} write at {} buf len {} off {} len {} length {}", _baseKey, position, buf.length, off,
          len, length);
      _crc64.update(position);
      _currentCrc.set(_crc64.update(buf, off, length));
      return _channel.write(ByteBuffer.wrap(buf, off, length), position);
    } finally {
      _lastWriteTime.set(System.nanoTime());
      _writeLock.unlock();
    }
  }

  @Override
  public void sync() {
    _writeLock.lock();
    try {
      while (!doSync()) {
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException ex) {
          return;
        }
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private boolean doSync() {
    try {
      if (_lastCrcSync.get() == _currentCrc.get()) {
        return true;
      }

      long crc = _currentCrc.get();
      String key = getS3Key(crc);
      PutObjectRequest putObjectRequest = new PutObjectRequest(_bucketName, key, _localCacheFile);
      LOGGER.debug("sync s3://{}/{} from {}", _bucketName, key, _localCacheFile);
      ObjectMetadata metadata = new ObjectMetadata();

      Map<String, String> userMetadata = ImmutableMap.of(BLOCK_CRC, Long.toString(crc));
      metadata.setUserMetadata(userMetadata);
      putObjectRequest.setMetadata(metadata);
      _client.putObject(putObjectRequest);
      _crcBlockManager.putBlockCrc(_blockId, crc);
      _lastCrcSync.set(crc);
      return true;
    } catch (Exception e) {
      LOGGER.error("Unknown error trying to sync", e);
      return false;
    }
  }

  private String getS3Key(long crc) {
    return _baseKey + "/" + crc;
  }

  public long getCurrentCrc() {
    return _currentCrc.get();
  }

  @Override
  public void close() {
    _writeLock.lock();
    try {
      sync();
      IOUtils.closeQuietly(_channel, _random);
      _localCacheFile.delete();
    } finally {
      _writeLock.unlock();
    }
  }

  private void load() throws Exception {
    long crc = getCurrentCrc();
    String key = getS3Key(crc);
    LOGGER.debug("load s3://{}/{} from {}", _bucketName, key, _localCacheFile);
    long current;
    long object;
    while ((current = getCurrentCrc()) != (object = getObjectCrc(key))) {
      LOGGER.warn("load - waiting until s3://{}/{} is consistent, CRC expecting {} object value {}", _bucketName, key,
          current, object);
      Thread.sleep(_consistencyWaitTime);
    }
    if (getCurrentCrc() == CRC64.DEFAULT_VALUE) {
      try (RandomAccessFile rand = new RandomAccessFile(_localCacheFile, RW)) {
        rand.setLength(_blockSize);
      }
    } else {
      GetObjectRequest getObjectRequest = new GetObjectRequest(_bucketName, key);
      _client.getObject(getObjectRequest, _localCacheFile);
    }
  }

  private long getObjectCrc(String key) throws IOException, FileNotFoundException {
    try {
      ObjectMetadata objectMetadata = _client.getObjectMetadata(_bucketName, key);
      return Long.parseLong(objectMetadata.getUserMetaDataOf(BLOCK_CRC));
    } catch (AmazonS3Exception e) {
      if (e.getErrorCode()
           .equals(_404_NOT_FOUND)) {
        return CRC64.DEFAULT_VALUE;
      }
      throw e;
    }
  }

}
