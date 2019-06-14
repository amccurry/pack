package pack.s3;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.collect.ImmutableMap;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;

public class S3Block implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3Block.class);

  private static final String _404_NOT_FOUND = "404 Not Found";
  private static final String BLOCK_CRC = "BLOCK_CRC";
  private static final String RW = "rw";

  public static void main(String[] args) throws Exception {

    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    File file = new File("./test.block");
    long sync;
    int blockId = 1001;
    int blockSize = 10_000_000;
    String bucketName = "<your bucket here>";
    String prefix = "testprefix";
    // long initCrc = -1L;
    long initCrc = 7836682694288837319L;

    while (true) {

      S3BlockConfig config = S3BlockConfig.builder()
                                          .blockId(blockId)
                                          .bucketName(bucketName)
                                          .blockSize(blockSize)
                                          .client(client)
                                          .currentCrc(initCrc)
                                          .localCacheFile(file)
                                          .prefix(prefix)
                                          .build();

      try (S3Block s3Block = new S3Block(config)) {
        {
          Random random = new Random();
          byte[] array = new byte[blockId];
          Arrays.fill(array, (byte) random.nextInt());
          ByteBuffer buffer = ByteBuffer.wrap(array);
          Pointer buf = Pointer.wrap(Runtime.getSystemRuntime(), buffer);
          s3Block.write(buf, 100, 100);
        }

        {
          byte[] array = new byte[blockId];
          ByteBuffer buffer = ByteBuffer.wrap(array);
          Pointer buf = Pointer.wrap(Runtime.getSystemRuntime(), buffer);
          s3Block.read(buf, 100, 100);

          for (int i = 0; i < 100; i++) {
            System.out.println(array[i]);
          }
        }

        sync = s3Block.sync();
      }

      System.out.println("==================");

      try (S3Block s3Block = new S3Block(config.toBuilder()
                                               .currentCrc(sync)
                                               .build())) {
        {
          byte[] array = new byte[blockId];
          ByteBuffer buffer = ByteBuffer.wrap(array);
          Pointer buf = Pointer.wrap(Runtime.getSystemRuntime(), buffer);
          s3Block.read(buf, 100, 100);

          for (int i = 0; i < 100; i++) {
            System.out.println(array[i]);
          }
          System.out.println(s3Block.getCurrentCrc());
          initCrc = s3Block.getCurrentCrc();
        }
      }
    }

  }

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
  private final String _key;
  private final S3BlockConfig _config;
  private final long _idleTime;

  public S3Block(S3BlockConfig config) throws Exception {
    _config = config;
    _idleTime = config.getIdleWriteTime();
    _bucketName = config.getBucketName();
    _key = config.getPrefix() + "/" + Long.toString(config.getBlockId());
    _blockSize = config.getBlockSize();
    _client = config.getClient();
    _currentCrc.set(config.getCurrentCrc());
    _lastCrcSync.set(config.getCurrentCrc());
    _crc64 = CRC64.newInstance(config.getCurrentCrc());
    _localCacheFile = config.getLocalCacheFile();
    load();
    _random = new RandomAccessFile(_localCacheFile, RW);
    _channel = _random.getChannel();
  }

  public S3BlockConfig getConfig() {
    return _config;
  }

  public int read(Pointer buf, int size, long relativeOffset) throws Exception {
    int len = (int) Math.min(size, _blockSize - relativeOffset);
    byte[] buffer = new byte[len];
    int read = _channel.read(ByteBuffer.wrap(buffer), relativeOffset);
    buf.put(0, buffer, 0, read);
    return read;
  }

  public int write(Pointer buf, int size, long relativeOffset) throws Exception {
    _writeLock.lock();
    try {
      int len = (int) Math.min(size, _blockSize - relativeOffset);
      byte[] buffer = new byte[len];
      buf.get(0, buffer, 0, len);
      _crc64.update(relativeOffset);
      _currentCrc.set(_crc64.update(buffer));
      return _channel.write(ByteBuffer.wrap(buffer), relativeOffset);
    } finally {
      _lastWriteTime.set(System.nanoTime());
      _writeLock.unlock();
    }
  }

  public void syncIfIdle() {
    if (isIdle()) {
      sync();
    }
  }

  private boolean isIdle() {
    return _lastWriteTime.get() + _idleTime < System.nanoTime();
  }

  public long sync() {
    _writeLock.lock();
    try {
      if (_lastCrcSync.get() == _currentCrc.get()) {
        return _currentCrc.get();
      }
      PutObjectRequest putObjectRequest = new PutObjectRequest(_bucketName, _key, _localCacheFile);
      ObjectMetadata metadata = new ObjectMetadata();
      long crc = _currentCrc.get();
      Map<String, String> userMetadata = ImmutableMap.of(BLOCK_CRC, Long.toString(crc));
      metadata.setUserMetadata(userMetadata);
      putObjectRequest.setMetadata(metadata);
      _client.putObject(putObjectRequest);
      _lastCrcSync.set(crc);
      return crc;
    } finally {
      _writeLock.unlock();
    }
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
    } finally {
      _writeLock.unlock();
    }
  }

  private void load() throws Exception {
    long current;
    long object;
    while ((current = getCurrentCrc()) != (object = getObjectCrc())) {
      LOGGER.info("Waiting until Object {} is consistent, CRC expecting {} object value {}", _key, current, object);
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    }
    if (getCurrentCrc() == -1L) {
      try (RandomAccessFile rand = new RandomAccessFile(_localCacheFile, "rw")) {
        rand.setLength(_blockSize);
      }
    } else {
      GetObjectRequest getObjectRequest = new GetObjectRequest(_bucketName, _key);
      _client.getObject(getObjectRequest, _localCacheFile);
    }
  }

  private long getObjectCrc() throws IOException, FileNotFoundException {
    try {
      ObjectMetadata objectMetadata = _client.getObjectMetadata(_bucketName, _key);
      return Long.parseLong(objectMetadata.getUserMetaDataOf(BLOCK_CRC));
    } catch (AmazonS3Exception e) {
      if (e.getErrorCode()
           .equals(_404_NOT_FOUND)) {
        return -1L;
      }
      throw e;
    }
  }

}
