package pack.distributed.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.thirdparty.ion.IonException;

import pack.distributed.storage.hdfs.PackHdfsReader;
import pack.distributed.storage.hdfs.ReadRequest;
import pack.distributed.storage.kafka.PackKafkaClientFactory;
import pack.distributed.storage.kafka.PackKafkaWriter;
import pack.distributed.storage.wal.PackWalCacheManager;
import pack.iscsi.storage.BaseStorageModule;
import pack.iscsi.storage.utils.PackUtils;

public class PackStorageModule extends BaseStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackStorageModule.class);

  private final AtomicReference<PackKafkaWriter> _packKafkaManager = new AtomicReference<PackKafkaWriter>();
  private final PackKafkaClientFactory _kafkaClientFactory;
  private final PackHdfsReader _hdfsReader;
  private final PackWalCacheManager _walCacheManager;
  private final Integer _topicPartition = 0;
  private final String _topic;

  public PackStorageModule(String name, PackMetaData metaData, Configuration conf, Path volumeDir,
      PackKafkaClientFactory kafkaClientFactory, UserGroupInformation ugi, File cacheDir) throws IOException {
    super(metaData.getLength(), metaData.getBlockSize(), name);
    _topic = PackConfig.getTopic(name);
    _kafkaClientFactory = kafkaClientFactory;
    _hdfsReader = new PackHdfsReader(conf, volumeDir, ugi);
    _walCacheManager = new PackWalCacheManager(name, cacheDir, _hdfsReader, metaData);
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    int blockOffset = getBlockOffset(storageIndex);
    int blockId = getBlockId(storageIndex);
    LOGGER.info("read boff {} len {} bid {} pos {}", blockOffset, bytes.length, blockId, storageIndex);
    List<ReadRequest> requests = createRequests(ByteBuffer.wrap(bytes), storageIndex);
    if (_walCacheManager.readBlocks(requests)) {
      _hdfsReader.readBlocks(requests);
    }
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    int len = bytes.length;
    int off = 0;
    long pos = storageIndex;
    int blockSize = _blockSize;
    PackKafkaWriter packKafkaManager = getPackKafkaManager();

    while (len > 0) {
      int blockOffset = getBlockOffset(pos);
      int blockId = getBlockId(pos);
      LOGGER.info("write bo {} bid {} rlen {} pos {}", blockOffset, blockId, len, pos);
      if (blockOffset != 0) {
        throw new IonException("block offset not 0");
      }
      packKafkaManager.write(getBlockId(pos), bytes, off, blockSize);
      len -= blockSize;
      off += blockSize;
      pos += blockSize;
    }
  }

  @Override
  public void flushWrites() throws IOException {
    getPackKafkaManager().flush();
  }

  @Override
  public void close() throws IOException {
    PackUtils.close(LOGGER, _packKafkaManager.get(), _walCacheManager, _hdfsReader);
  }

  private PackKafkaWriter getPackKafkaManager() {
    PackKafkaWriter packKafkaWriter = _packKafkaManager.get();
    if (packKafkaWriter == null) {
      return createPackKafkaWriter();
    }
    return packKafkaWriter;
  }

  private synchronized PackKafkaWriter createPackKafkaWriter() {
    PackKafkaWriter packKafkaWriter = _packKafkaManager.get();
    if (packKafkaWriter == null) {
      _packKafkaManager.set(
          packKafkaWriter = new PackKafkaWriter(_kafkaClientFactory.createProducer(), _topic, _topicPartition));
    }
    return packKafkaWriter;
  }

  public List<ReadRequest> createRequests(ByteBuffer byteBuffer, long storageIndex) {
    int remaining = byteBuffer.remaining();
    int bufferPosition = 0;
    List<ReadRequest> result = new ArrayList<>();
    while (remaining > 0) {
      int blockOffset = getBlockOffset(storageIndex);
      int blockId = getBlockId(storageIndex);
      int len = Math.min(_blockSize - blockOffset, remaining);

      byteBuffer.position(bufferPosition);
      byteBuffer.limit(bufferPosition + len);

      ByteBuffer slice = byteBuffer.slice();
      result.add(new ReadRequest(blockId, blockOffset, slice));

      storageIndex += len;
      bufferPosition += len;
      remaining -= len;
    }
    return result;
  }

}
