package pack.iscsi.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.io.BytesWritable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.block.blockstore.hdfs.blockstore.WalFile;
import pack.block.blockstore.hdfs.blockstore.WalFile.Writer;
import pack.block.blockstore.hdfs.file.WalKeyWritable;
import pack.iscsi.storage.concurrent.Executors;
import pack.iscsi.storage.kafka.PackKafkaManager;
import pack.iscsi.storage.utils.IOUtils;

public class DataSyncManager implements DataArchiveManager, Closeable {

  private final static Logger LOGGER = LoggerFactory.getLogger(DataSyncManager.class);

  private final AtomicReference<OffsetInfo> _currentEndOffset = new AtomicReference<>();
  private final AtomicReference<OffsetInfo> _currentProcessOffset = new AtomicReference<>();
  private final KafkaProducer<Long, byte[]> _producer;
  private final ExecutorService _executorService = Executors.newCachedThreadPool("datasync");
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final PackStorageMetaData _metaData;
  private final AtomicReference<List<WalCache>> _currentLocalWalCacheList = new AtomicReference<>();
  private final Object _walListLock = new Object();
  private final AtomicReference<WalCache> _currentLocalWalCache = new AtomicReference<WalCache>();
  private final SynchronousQueue<WalCache> _nextWalCache = new SynchronousQueue<>();
  private final int _maxOffsetPerWalFile;
  private final Future<Void> _localWalConsumerFuture;
  private final Future<Void> _remoteWalConsumerFuture;
  private final Future<Void> _walFuture;
  private final PackKafkaManager _kafkaManager;
  private final long _maxOffsetLagDiff;
  private final long _lagSyncPollWaitTime;
  private final DataArchiveManager _dataArchiveManager;
  private final AtomicReference<Writer> _currentRemoteWalWriter = new AtomicReference<WalFile.Writer>();
  private final DelayedResourceCleanup _delayedResourceCleanup;

  public DataSyncManager(DelayedResourceCleanup delayedResourceCleanup, PackKafkaManager kafkaManager,
      PackStorageMetaData metaData) {
    this(delayedResourceCleanup, kafkaManager, metaData, null);
  }

  public DataSyncManager(DelayedResourceCleanup delayedResourceCleanup, PackKafkaManager kafkaManager,
      PackStorageMetaData metaData, DataArchiveManager manager) {
    _delayedResourceCleanup = delayedResourceCleanup;
    _metaData = metaData;
    _maxOffsetLagDiff = metaData.getMaxOffsetLagDiff();
    _lagSyncPollWaitTime = metaData.getLagSyncPollWaitTime();
    _kafkaManager = kafkaManager;
    _maxOffsetPerWalFile = _metaData.getMaxOffsetPerWalFile();
    _producer = kafkaManager.createProducer(metaData.getKafkaTopic());
    _localWalConsumerFuture = _executorService.submit(new LocalWalConsumer());
    _walFuture = _executorService.submit(new WalFileGenerator());
    if (manager == null) {
      _remoteWalConsumerFuture = null;
      _dataArchiveManager = null;
    } else {
      _remoteWalConsumerFuture = _executorService.submit(new RemoteWalConsumer());
      _dataArchiveManager = manager;
    }
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    IOUtils.closeQuietly(_localWalConsumerFuture, _walFuture, _remoteWalConsumerFuture);
    _executorService.shutdownNow();
    IOUtils.closeQuietly(_producer);
    IOUtils.closeQuietly(_kafkaManager);
  }

  public List<WalCache> getPackWalCacheList() {
    return _currentLocalWalCacheList.get();
  }

  public void checkState() {
    IOUtils.checkFutureIsRunning(_localWalConsumerFuture);
    IOUtils.checkFutureIsRunning(_remoteWalConsumerFuture);
    IOUtils.checkFutureIsRunning(_walFuture);
  }

  /**
   * This method makes sure that the sync is up to date before allowing reads to
   * proceed. It relies on kafka offsets.
   * 
   * @throws InterruptedException
   */
  public void waitForKafkaSyncIfNeeded() throws InterruptedException {
    while (_running.get()) {
      OffsetInfo endOffsetInfo = _currentEndOffset.get();
      if (endOffsetInfo == null) {
        Thread.sleep(_lagSyncPollWaitTime);
        continue;
      }
      OffsetInfo processOffsetInfo = _currentProcessOffset.get();
      if (processOffsetInfo == null) {
        Thread.sleep(_lagSyncPollWaitTime);
        continue;
      }
      long diff = (endOffsetInfo.getOffset() - 1) - processOffsetInfo.getOffset();
      LOGGER.debug("end offset {} process offset {}", endOffsetInfo, processOffsetInfo);
      if (diff > _maxOffsetLagDiff) {
        Thread.sleep(_lagSyncPollWaitTime);
        continue;
      }
      return;
    }
  }

  public long commit(long commitOffset) throws IOException {
    LOGGER.info("Committing wal file at offset {}", commitOffset);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    TopicPartition topicPartition = new TopicPartition(_metaData.getKafkaTopic(), _metaData.getKafkaPartition());
    OffsetAndMetadata offsetAndMetaData = new OffsetAndMetadata(commitOffset);
    offsets.put(topicPartition, offsetAndMetaData);
    long newOffset;
    try (KafkaConsumer<Long, byte[]> consumer = _kafkaManager.createConsumer(_metaData.getKafkaTopic())) {
      consumer.commitSync(offsets);
      OffsetAndMetadata metadata = consumer.committed(topicPartition);
      newOffset = metadata.offset();
    }
    synchronized (_walListLock) {
      List<WalCache> list = _currentLocalWalCacheList.get();
      Builder<WalCache> inUseBuilder = ImmutableList.builder();
      Builder<WalCache> notInUseBuilder = ImmutableList.builder();
      for (WalCache walCache : list) {
        if (walStillInUse(walCache, commitOffset)) {
          inUseBuilder.add(walCache);
        } else {
          notInUseBuilder.add(walCache);
        }
      }
      _currentLocalWalCacheList.set(inUseBuilder.build());
      addToWalGc(notInUseBuilder.build());
      return newOffset;
    }
  }

  private void addToWalGc(ImmutableList<WalCache> walToBeClosed) {
    Closeable[] closeables = walToBeClosed.toArray(new Closeable[] {});
    _delayedResourceCleanup.register(walToBeClosed, closeables);
  }

  private boolean walStillInUse(WalCache walCache, long commitOffset) {
    if (commitOffset <= walCache.getEndingOffset()) {
      return true;
    }
    return false;
  }

  private boolean shouldRollWalCache(WalCache localWalCache, long offset) {
    if (localWalCache == null) {
      return true;
    }
    long startingOffset = localWalCache.getStartingOffset();
    if ((offset - startingOffset) >= _maxOffsetPerWalFile) {
      return true;
    }
    return false;
  }

  private WalCache getLocalWalCacheForWriting(long offset) throws InterruptedException {
    WalCache localWalCache = _currentLocalWalCache.get();
    if (shouldRollWalCache(localWalCache, offset)) {
      synchronized (_walListLock) {
        localWalCache = _nextWalCache.take();
        List<WalCache> curList = _currentLocalWalCacheList.get();
        List<WalCache> newList = new ArrayList<>();
        newList.add(localWalCache);
        if (curList != null) {
          newList.addAll(curList);
        }
        _currentLocalWalCacheList.set(newList);
        _currentLocalWalCache.set(localWalCache);
      }
    }
    return localWalCache;
  }

  @Override
  public BlockReader getBlockReader() throws IOException {
    return BlockReader.mergeInOrder(_currentLocalWalCacheList.get());
  }

  public KafkaProducer<Long, byte[]> getKafkaProducer() {
    return _producer;
  }

  class WalFileGenerator implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      while (_running.get()) {
        try {
          walFileGenerator();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        }
      }
      return null;
    }
  }

  class RemoteWalConsumer implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      while (_running.get()) {
        try {
          remoteWalConsumer();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        }
      }
      return null;
    }
  }

  class LocalWalConsumer implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      while (_running.get()) {
        try {
          return localWalConsumer();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        }
      }
      return null;
    }
  }

  private void walFileGenerator() throws IOException, InterruptedException {
    File dir = new File(_metaData.getLocalWalCachePath());
    dir.mkdirs();
    TopicPartition partition = new TopicPartition(_metaData.getKafkaTopic(), _metaData.getKafkaPartition());
    long startingOffset;
    try (KafkaConsumer<Long, byte[]> consumer = _kafkaManager.createConsumer(_metaData.getKafkaTopic())) {
      OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
      if (offsetAndMetadata != null) {
        startingOffset = offsetAndMetadata.offset();
      } else {
        startingOffset = 0;
      }
    }
    int maxOffsetPerWalFile = _metaData.getMaxOffsetPerWalFile();
    while (_running.get()) {
      File file = new File(dir, startingOffset + ".wal");
      WalCache cache = new WalCache(startingOffset, maxOffsetPerWalFile, file, _metaData.getLengthInBytes(),
          _metaData.getBlockSize(), _metaData.getWalCacheMemorySize());
      LOGGER.info("Created new wal file adding to next wal queue {}", file);
      _nextWalCache.put(cache);
      startingOffset += maxOffsetPerWalFile;
    }
  }

  private Void localWalConsumer() throws InterruptedException, IOException {
    try (KafkaConsumer<Long, byte[]> consumer = _kafkaManager.createConsumer(_metaData.getKafkaTopic())) {
      TopicPartition partition = new TopicPartition(_metaData.getKafkaTopic(), _metaData.getKafkaPartition());
      OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
      long startingOffset;
      if (offsetAndMetadata != null) {
        startingOffset = offsetAndMetadata.offset();
      } else {
        startingOffset = 0;
      }
      int blockSize = _metaData.getBlockSize();
      consumer.assign(ImmutableList.of(partition));
      consumer.seek(partition, startingOffset);
      updateProcessOffset(startingOffset);
      while (_running.get()) {
        LOGGER.debug("Starting poll for records.");
        updateKnownEndOffset(consumer, partition);
        ConsumerRecords<Long, byte[]> records = consumer.poll(TimeUnit.SECONDS.toMillis(1));
        for (ConsumerRecord<Long, byte[]> record : records) {
          long kafkaOffset = record.offset();
          byte[] value = record.value();
          long position = record.key();
          LOGGER.debug("new record position {} length {}", position, value.length);
          int length = value.length;
          int offset = 0;

          while (length > 0) {
            long blockId = IOUtils.getBlockId(position, blockSize);
            ByteBuffer byteBuffer = ByteBuffer.wrap(value, offset, blockSize);
            WalCache localWalCache = getLocalWalCacheForWriting(kafkaOffset);
            localWalCache.write(blockId, byteBuffer);
            offset += blockSize;
            position += blockSize;
            length -= blockSize;
          }
          updateProcessOffset(kafkaOffset);
        }
      }
      return null;
    }
  }

  private void updateProcessOffset(long offset) {
    _currentProcessOffset.set(OffsetInfo.builder()
                                        .offset(offset)
                                        .timestamp(System.currentTimeMillis())
                                        .build());
  }

  private void remoteWalConsumer() throws IOException, InterruptedException {
    try (KafkaConsumer<Long, byte[]> consumer = _kafkaManager.createConsumer(_metaData.getKafkaTopic())) {
      TopicPartition partition = new TopicPartition(_metaData.getKafkaTopic(), _metaData.getKafkaPartition());
      long lastCommitOffset = getLastCommit(consumer, partition);
      int blockSize = _metaData.getBlockSize();
      consumer.assign(ImmutableList.of(partition));
      consumer.seek(partition, lastCommitOffset);
      BytesWritable val = new BytesWritable();
      long lastWriteTime = 0L;
      while (_running.get()) {
        // try to commit new blocks from archive
        long commitOffset = _dataArchiveManager.getMaxCommitOffset();
        if (commitOffset > lastCommitOffset) {
          commit(commitOffset - 1);
          lastCommitOffset = commitOffset;
        }

        OffsetInfo endOffsetInfo = _currentEndOffset.get();
        if (endOffsetInfo == null) {
          Thread.sleep(TimeUnit.SECONDS.toMillis(10));
          continue;
        }

        LOGGER.debug("Starting poll for records.");
        ConsumerRecords<Long, byte[]> records = consumer.poll(TimeUnit.SECONDS.toMillis(10));
        if (records.isEmpty()) {
          if (lastWriteTime + TimeUnit.SECONDS.toNanos(10) < System.nanoTime()) {
            closeCurrentRemoteWalWriter();
          }
        } else {
          for (ConsumerRecord<Long, byte[]> record : records) {
            long kafkaOffset = record.offset();
            WalFile.Writer walFileWriter = getRemoteWalWriter(kafkaOffset);
            byte[] value = record.value();
            long position = record.key();
            LOGGER.debug("new record position {} length {}", position, value.length);
            int length = value.length;
            int offset = 0;
            while (length > 0) {
              long blockId = IOUtils.getBlockId(position, blockSize);
              val.set(value, offset, blockSize);
              walFileWriter.append(new WalKeyWritable(blockId), val);
              offset += blockSize;
              position += blockSize;
              length -= blockSize;
            }
            lastWriteTime = System.nanoTime();
          }
        }
      }
    }
  }

  public Writer getRemoteWalWriter(long kafkaOffset) throws IOException {
    Writer writer = _currentRemoteWalWriter.get();
    if (shouldRollRemoteWalWriter(writer, kafkaOffset)) {
      if (writer != null) {
        LOGGER.info("Rolling remote wal writer {}", writer);
        writer.close();
      }
      _currentRemoteWalWriter.set(writer = _dataArchiveManager.createRemoteWalWriter(kafkaOffset));
    }
    return writer;
  }

  private void closeCurrentRemoteWalWriter() throws IOException {
    Writer writer = _currentRemoteWalWriter.get();
    if (writer != null) {
      LOGGER.info("Close wal writer {}", writer);
      writer.close();
    }
    _currentRemoteWalWriter.set(null);
  }

  private boolean shouldRollRemoteWalWriter(Writer writer, long kafkaOffset) {
    if (writer == null) {
      return true;
    } else if (kafkaOffset % _maxOffsetPerWalFile == 0) {
      return true;
    } else {
      return false;
    }
  }

  private long getLastCommit(KafkaConsumer<Long, byte[]> consumer, TopicPartition partition) {
    long startingOffset;
    OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
    if (offsetAndMetadata != null) {
      startingOffset = offsetAndMetadata.offset();
    } else {
      startingOffset = 0;
    }
    return startingOffset;
  }

  private void updateKnownEndOffset(KafkaConsumer<Long, byte[]> consumer, TopicPartition partition) {
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(ImmutableList.of(partition));
    Long endOffset = endOffsets.get(partition);
    _currentEndOffset.set(OffsetInfo.builder()
                                    .offset(getOffset(endOffset))
                                    .timestamp(System.currentTimeMillis())
                                    .build());
  }

  private long getOffset(Long endOffset) {
    long offset;
    if (endOffset == null) {
      offset = 0;
    } else {
      offset = endOffset;
    }
    return offset;
  }

  @Override
  public Writer createRemoteWalWriter(long kafkaOffset) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public long getMaxCommitOffset() {
    throw new RuntimeException("Not supported");
  }

}
