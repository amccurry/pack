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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import pack.iscsi.storage.kafka.PackKafkaManager;
import pack.iscsi.storage.utils.IOUtils;

public class DataSyncManager implements DataArchiveManager, Closeable {

  private final static Logger LOGGER = LoggerFactory.getLogger(DataSyncManager.class);

  private final AtomicReference<OffsetInfo> _currentEndOffset = new AtomicReference<>();
  private final AtomicReference<OffsetInfo> _currentProcessOffset = new AtomicReference<>();
  private final KafkaProducer<Long, byte[]> _producer;
  private final ExecutorService _executorService = Executors.newCachedThreadPool();
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final PackStorageMetaData _metaData;
  private final AtomicReference<List<WalCache>> _currentWalCacheList = new AtomicReference<>();
  private final Object _walListLock = new Object();
  private final AtomicReference<WalCache> _currentWalCache = new AtomicReference<WalCache>();
  private final SynchronousQueue<WalCache> _nextWalCache = new SynchronousQueue<>();
  private final int _maxOffsetPerWalFile;
  private final Future<Void> _consumerFuture;
  private final Future<Void> _walFuture;
  private final PackKafkaManager _kafkaManager;
  private final long _maxOffsetLagDiff;
  private final long _lagSyncPollWaitTime;

  public DataSyncManager(PackKafkaManager kafkaManager, PackStorageMetaData metaData) {
    _metaData = metaData;
    _maxOffsetLagDiff = metaData.getMaxOffsetLagDiff();
    _lagSyncPollWaitTime = metaData.getLagSyncPollWaitTime();
    _kafkaManager = kafkaManager;
    _maxOffsetPerWalFile = _metaData.getMaxOffsetPerWalFile();
    _producer = kafkaManager.createProducer(metaData.getKafkaTopic());
    _consumerFuture = _executorService.submit(new ConsumerWriter());
    _walFuture = _executorService.submit(new WalFileGenerator());
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    _consumerFuture.cancel(true);
    _walFuture.cancel(true);
    _executorService.shutdownNow();
    IOUtils.closeQuietly(_producer);
    IOUtils.closeQuietly(_kafkaManager);
  }

  public List<WalCache> getPackWalCacheList() {
    return _currentWalCacheList.get();
  }

  public void checkState() {
    IOUtils.checkFutureIsRunning(_consumerFuture);
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
      LOGGER.info("end offset {} process offset {}", endOffsetInfo, processOffsetInfo);
      if (diff > _maxOffsetLagDiff) {
        Thread.sleep(_lagSyncPollWaitTime);
        continue;
      }
      return;
    }
  }

  public long commit(WalCache packWalCache) throws IOException {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    TopicPartition topicPartition = new TopicPartition(_metaData.getKafkaTopic(), _metaData.getKafkaPartition());
    long endingOffset = packWalCache.getEndingOffset();
    OffsetAndMetadata offsetAndMetaData = new OffsetAndMetadata(endingOffset);
    offsets.put(topicPartition, offsetAndMetaData);
    long newOffset;
    try (KafkaConsumer<Long, byte[]> consumer = _kafkaManager.createConsumer(_metaData.getKafkaTopic())) {
      consumer.commitSync(offsets);
      OffsetAndMetadata metadata = consumer.committed(topicPartition);
      newOffset = metadata.offset();
    }
    WalCache localWalCache;
    synchronized (_walListLock) {
      List<WalCache> list = _currentWalCacheList.get();
      localWalCache = list.get(list.size() - 1);
      if (localWalCache.getEndingOffset() == endingOffset) {
        ImmutableList<WalCache> newList = ImmutableList.copyOf(list.subList(0, list.size() - 1));
        _currentWalCacheList.set(newList);
        IOUtils.closeQuietly(localWalCache);
        return newOffset;
      } else {
        throw new IOException(
            "Something has gone wrong, offset " + endingOffset + " was being committed but is not the last wal file.");
      }
    }
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
    WalCache localWalCache = _currentWalCache.get();
    if (shouldRollWalCache(localWalCache, offset)) {
      synchronized (_walListLock) {
        localWalCache = _nextWalCache.take();
        List<WalCache> curList = _currentWalCacheList.get();
        List<WalCache> newList = new ArrayList<>();
        newList.add(localWalCache);
        if (curList != null) {
          newList.addAll(curList);
        }
        _currentWalCacheList.set(newList);
        _currentWalCache.set(localWalCache);
      }
    }
    return localWalCache;
  }

  @Override
  public BlockReader getBlockReader() throws IOException {
    return BlockReader.mergeInOrder(_currentWalCacheList.get());
  }

  public KafkaProducer<Long, byte[]> getKafkaProducer() {
    return _producer;
  }

  class WalFileGenerator implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      while (_running.get()) {
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
      return null;
    }
  }

  class ConsumerWriter implements Callable<Void> {
    @Override
    public Void call() throws Exception {
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
          ConsumerRecords<Long, byte[]> records = consumer.poll(TimeUnit.MILLISECONDS.toMillis(10));
          for (ConsumerRecord<Long, byte[]> record : records) {
            long kafkaOffset = record.offset();
            byte[] value = record.value();
            long position = record.key();
            LOGGER.info("new record position {} length {}", position, value.length);
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

}
