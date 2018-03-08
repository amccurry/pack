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
import pack.distributed.storage.kafka.PackKafkaReader;
import pack.distributed.storage.kafka.PackKafkaWriter;
import pack.distributed.storage.trace.PackTracer;
import pack.distributed.storage.wal.PackWalCacheManager;
import pack.iscsi.storage.BaseStorageModule;
import pack.iscsi.storage.utils.PackUtils;

public class PackStorageModule extends BaseStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackStorageModule.class);

  private final AtomicReference<PackKafkaWriter> _packKafkaWriter = new AtomicReference<PackKafkaWriter>();
  private final PackKafkaClientFactory _kafkaClientFactory;
  private final PackHdfsReader _hdfsReader;
  private final PackWalCacheManager _walCacheManager;
  private final Integer _topicPartition = 0;
  private final String _topic;
  private final PackKafkaReader _packKafkaReader;

  public PackStorageModule(String name, String serialId, PackMetaData metaData, Configuration conf, Path volumeDir,
      PackKafkaClientFactory kafkaClientFactory, UserGroupInformation ugi, File cacheDir) throws IOException {
    super(metaData.getLength(), metaData.getBlockSize(), name);
    _topic = metaData.getTopicId();
    _kafkaClientFactory = kafkaClientFactory;
    _hdfsReader = new PackHdfsReader(conf, volumeDir, ugi);
    _walCacheManager = new PackWalCacheManager(name, cacheDir, _hdfsReader, metaData, conf, volumeDir);
    _packKafkaReader = new PackKafkaReader(name, serialId, _kafkaClientFactory, _walCacheManager, _hdfsReader, _topic,
        _topicPartition);
    _packKafkaReader.start();
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    try (PackTracer tracer = PackTracer.create(LOGGER, "read")) {
      _packKafkaReader.waitForWalSync(tracer);
      int blockOffset = getBlockOffset(storageIndex);
      int blockId = getBlockId(storageIndex);
      int length = bytes.length;
      if (length == 0) {
        return;
      }
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("read bo {} bid {} rlen {} pos {}", blockOffset, blockId, length, storageIndex);
      }
      List<ReadRequest> requests = createRequests(ByteBuffer.wrap(bytes), storageIndex);
      boolean moreToRead;
      try (PackTracer span = tracer.span(LOGGER, "wal cache read")) {
        moreToRead = _walCacheManager.readBlocks(requests);
      }
      if (moreToRead) {
        try (PackTracer span = tracer.span(LOGGER, "block read")) {
          _hdfsReader.readBlocks(requests);
        }
      }
    }
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    try (PackTracer tracer = PackTracer.create(LOGGER, "write")) {
      int len = bytes.length;
      int off = 0;
      long pos = storageIndex;
      int blockSize = _blockSize;
      PackKafkaWriter packKafkaWriter = getPackKafkaWriter();

      while (len > 0) {
        int blockOffset = getBlockOffset(pos);
        int blockId = getBlockId(pos);
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("write bo {} bid {} rlen {} pos {}", blockOffset, blockId, len, pos);
        }
        if (blockOffset != 0) {
          throw new IonException("block offset not 0");
        }
        packKafkaWriter.write(tracer, getBlockId(pos), bytes, off, blockSize);
        len -= blockSize;
        off += blockSize;
        pos += blockSize;
      }
    }
  }

  @Override
  public void flushWrites() throws IOException {
    try (PackTracer tracer = PackTracer.create(LOGGER, "flush")) {
      getPackKafkaWriter().flush(tracer);
    }
  }

  @Override
  public void close() throws IOException {
    PackUtils.close(LOGGER, _packKafkaWriter.get(), _packKafkaReader, _walCacheManager, _hdfsReader);
  }

  private PackKafkaWriter getPackKafkaWriter() {
    PackKafkaWriter packKafkaWriter = _packKafkaWriter.get();
    if (packKafkaWriter == null) {
      return createPackKafkaWriter();
    }
    return packKafkaWriter;
  }

  private synchronized PackKafkaWriter createPackKafkaWriter() {
    PackKafkaWriter packKafkaWriter = _packKafkaWriter.get();
    if (packKafkaWriter == null) {
      _packKafkaWriter.set(
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
