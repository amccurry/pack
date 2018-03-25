package pack.distributed.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.hdfs.HdfsBlockGarbageCollector;
import pack.distributed.storage.hdfs.PackHdfsReader;
import pack.distributed.storage.kafka.PackKafkaClientFactory;
import pack.distributed.storage.kafka.PackKafkaReader;
import pack.distributed.storage.kafka.PackKafkaWriter;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.read.ReadRequest;
import pack.distributed.storage.status.ServerStatusManager;
import pack.distributed.storage.trace.PackTracer;
import pack.distributed.storage.wal.PackWalCacheFactory;
import pack.distributed.storage.wal.PackWalCacheManager;
import pack.distributed.storage.wal.WalCacheFactory;
import pack.distributed.storage.wal.WalCacheManager;
import pack.distributed.storage.zk.ZooKeeperClient;
import pack.iscsi.storage.BaseStorageModule;
import pack.iscsi.storage.utils.PackUtils;

public class PackStorageModule extends BaseStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackStorageModule.class);

  private final AtomicReference<PackKafkaWriter> _packKafkaWriter = new AtomicReference<PackKafkaWriter>();
  private final PackKafkaClientFactory _kafkaClientFactory;
  private final PackHdfsReader _hdfsReader;
  private final WalCacheManager _walCacheManager;
  private final Integer _topicPartition = 0;
  private final String _topic;
  private final PackKafkaReader _packKafkaReader;
  private final WriteBlockMonitor _writeBlockMonitor;
  private final WalCacheFactory _cacheFactory;
  private final UUID _serialId;
  private final String _name;
  private final ServerStatusManager _serverStatusManager;
  private final Object _lock = new Object();
  private final ZooKeeperClient _zk;
  private final String _targetHostAddress;

  public PackStorageModule(String name, PackMetaData metaData, Configuration conf, Path volumeDir,
      PackKafkaClientFactory kafkaClientFactory, UserGroupInformation ugi, File cacheDir,
      WriteBlockMonitor writeBlockMonitor, ServerStatusManager serverStatusManager,
      HdfsBlockGarbageCollector hdfsBlockGarbageCollector, long maxWalSize, long maxWalLifeTime, ZooKeeperClient zk,
      String targetHostAddress) throws IOException {
    super(metaData.getLength(), metaData.getBlockSize());
    _zk = zk;
    _targetHostAddress = targetHostAddress;
    _name = name;
    _serialId = UUID.fromString(metaData.getSerialId());
    _topic = metaData.getTopicId();
    _kafkaClientFactory = kafkaClientFactory;
    _hdfsReader = new PackHdfsReader(conf, volumeDir, ugi, hdfsBlockGarbageCollector);
    _hdfsReader.refresh();
    _writeBlockMonitor = writeBlockMonitor;
    _serverStatusManager = serverStatusManager;
    _cacheFactory = new PackWalCacheFactory(metaData, cacheDir);
    _walCacheManager = new PackWalCacheManager(name, _writeBlockMonitor, _cacheFactory, _hdfsReader,
        _serverStatusManager, metaData, conf, volumeDir, maxWalSize, maxWalLifeTime);
    _packKafkaReader = new PackKafkaReader(name, metaData.getSerialId(), _kafkaClientFactory, _walCacheManager,
        _hdfsReader, _topic, _topicPartition);
    _packKafkaReader.start();
  }

  @Override
  public UUID getSerialId() {
    return _serialId;
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    synchronized (_lock) {
      try (PackTracer tracer = PackTracer.create(LOGGER, "read")) {
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
        for (ReadRequest request : requests) {
          if (_writeBlockMonitor.waitIfNeededForSync(request.getBlockId())) {
            fullSyncAndClearMonitor();
          }
        }
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
  }

  private void fullSyncAndClearMonitor() throws IOException {
    _writeBlockMonitor.clearAllLocks();
    _packKafkaReader.sync();
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    synchronized (_lock) {
      setupSessionWritablity();
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
            throw new IOException("block offset not 0");
          }
          packKafkaWriter.write(tracer, blockId, bytes, off, blockSize);
          len -= blockSize;
          off += blockSize;
          pos += blockSize;
        }
      }
    }
  }

  private void setupSessionWritablity() throws IOException {
    try {
      PackStorageTargetManager.checkSessionWritablity(_zk, _targetHostAddress, _name);
    } catch (IOException | KeeperException | InterruptedException e) {
      LOGGER.error("Unknown error while checking for writablity", e);
      PackStorageTargetManager.setSessionWritable(false);
      throw new IOException("Read only mode.");
    }
  }

  @Override
  public void flushWrites() throws IOException {
    synchronized (_lock) {
      try (PackTracer tracer = PackTracer.create(LOGGER, "flush")) {
        getPackKafkaWriter().flush(tracer);
      }
    }
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("Closing storage module {}", _name);
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
      _packKafkaWriter.set(packKafkaWriter = new PackKafkaWriter(_name, _kafkaClientFactory.createProducer(), _topic,
          _topicPartition, _writeBlockMonitor, _serverStatusManager));
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
