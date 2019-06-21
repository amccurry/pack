package pack.block.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Joiner;

import pack.block.CrcBlockManager;
import pack.block.util.CRC64;

public class S3CrcBlockManager implements CrcBlockManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3CrcBlockManager.class);

  private static final String SYNCS = "syncs";
  private static final Joiner KEY_JOINER = Joiner.on('/');

  private static final int GROWTH_MIN = 1024;
  private static final int INITIAL_SIZE = 2048;
  private final String _bucketName;
  private final AmazonS3 _client;
  private final AtomicReference<AtomicLongArray> _ref = new AtomicReference<AtomicLongArray>();
  private final CRC64 _crc64;
  private final WriteLock _writeSyncLock;
  private final ReadLock _readSyncLock;
  private final AtomicLong _currentCrc = new AtomicLong();
  private final AtomicLong _lastCrcSync = new AtomicLong();
  private final String _baseKey;
  private final long _consistencyWaitTime = TimeUnit.SECONDS.toMillis(5);
  private final CrcBlockManager _crcBlockManager;

  public S3CrcBlockManager(S3CrcBlockManagerConfig config) throws Exception {
    _client = config.getClient();
    _bucketName = config.getBucketName();

    _crcBlockManager = config.getCrcBlockManager();

    if (config.getPrefix() == null) {
      _baseKey = KEY_JOINER.join(config.getVolume(), SYNCS);
    } else {
      _baseKey = KEY_JOINER.join(config.getPrefix(), config.getVolume(), SYNCS);
    }

    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeSyncLock = reentrantReadWriteLock.writeLock();
    _readSyncLock = reentrantReadWriteLock.readLock();

    long initial = _crcBlockManager.getBlockCrc(0);
    _crc64 = CRC64.newInstance(initial);
    _ref.set(load(initial));
  }

  private AtomicLongArray load(long initalCrc) throws Exception {
    if (initalCrc == CRC64.DEFAULT_VALUE) {
      return new AtomicLongArray(INITIAL_SIZE);
    }
    String key = getS3Key(initalCrc);
    LOGGER.debug("load s3://{}/{} from {}", _bucketName, key);
    GetObjectRequest getObjectRequest = new GetObjectRequest(_bucketName, key);
    S3Object object = S3Utils.getObject(_client, getObjectRequest, _consistencyWaitTime);
    try (DataInputStream input = new DataInputStream(object.getObjectContent())) {
      return read(input);
    }
  }

  @Override
  public long getBlockCrc(long blockId) throws Exception {
    AtomicLongArray longArray = getAtomicLongArray(blockId);
    _readSyncLock.lock();
    try {
      return longArray.get((int) blockId);
    } finally {
      _readSyncLock.unlock();
    }
  }

  @Override
  public void putBlockCrc(long blockId, long crc) throws Exception {
    AtomicLongArray longArray = getAtomicLongArray(blockId);
    _readSyncLock.lock();
    try {
      _crc64.update(blockId);
      _currentCrc.set(_crc64.update(crc));
      longArray.set((int) blockId, crc);
    } finally {
      _readSyncLock.unlock();
    }
  }

  @Override
  public void sync() {
    _writeSyncLock.lock();
    try {
      while (!doSync()) {
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException ex) {
          return;
        }
      }
    } finally {
      _writeSyncLock.unlock();
    }
  }

  @Override
  public void close() {
    sync();
  }

  private boolean doSync() {
    try {
      if (_lastCrcSync.get() == _currentCrc.get()) {
        return true;
      }
      long crc = _currentCrc.get();
      String key = getS3Key(crc);
      byte[] data = getCrcData();
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(data.length);
      PutObjectRequest putObjectRequest = new PutObjectRequest(_bucketName, key, new ByteArrayInputStream(data),
          metadata);
      LOGGER.debug("sync s3://{}/{}", _bucketName, key);
      _client.putObject(putObjectRequest);
      _crcBlockManager.putBlockCrc(0, crc);
      _lastCrcSync.set(crc);
      return true;
    } catch (Exception e) {
      LOGGER.error("Unknown error trying to sync", e);
      return false;
    }
  }

  private byte[] getCrcData() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(baos)) {
      write(output, _ref.get());
    }
    return baos.toByteArray();
  }

  private AtomicLongArray read(DataInput input) throws IOException {
    int length = input.readInt();
    AtomicLongArray array = new AtomicLongArray(length);
    for (int i = 0; i < length; i++) {
      array.set(i, input.readLong());
    }
    return array;
  }

  private void write(DataOutput output, AtomicLongArray array) throws IOException {
    int length = array.length();
    output.writeInt(length);
    for (int i = 0; i < length; i++) {
      output.writeLong(array.get(i));
    }
  }

  private String getS3Key(long crc) {
    return _baseKey + "/" + crc;
  }

  private AtomicLongArray getAtomicLongArray(long blockId) {
    _writeSyncLock.lock();
    try {
      AtomicLongArray longArray = _ref.get();
      if (longArray == null) {
        _ref.set(longArray = new AtomicLongArray(INITIAL_SIZE));
      }
      while (longArray.length() <= blockId) {
        AtomicLongArray newLongArray = new AtomicLongArray(longArray.length() + GROWTH_MIN);
        copy(longArray, newLongArray);
        _ref.set(longArray = newLongArray);
      }
      return longArray;
    } finally {
      _writeSyncLock.unlock();
    }
  }

  private void copy(AtomicLongArray src, AtomicLongArray dst) {
    for (int i = 0; i < src.length(); i++) {
      dst.set(i, src.get(i));
    }
  }
}
