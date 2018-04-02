package pack.distributed.storage.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import pack.distributed.storage.broadcast.Block;
import pack.distributed.storage.broadcast.Blocks;
import pack.distributed.storage.broadcast.PackBroadcastReader;
import pack.distributed.storage.hdfs.MaxBlockLayer;
import pack.distributed.storage.wal.WalCacheManager;
import pack.iscsi.storage.utils.PackUtils;

public class PackKafkaReader extends PackBroadcastReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackKafkaReader.class);

  private final PackKafkaClientFactory _kafkaClientFactory;
  private final MaxBlockLayer _maxBlockLayer;
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final String _serialId;
  private final String _topic;
  private final Integer _partition;
  private final AtomicLong _endOffset = new AtomicLong(Long.MAX_VALUE);
  private final EndPointLookup _endPointLookup;
  private final long _kafkaPollTimeout;
  private final long _syncDelay;

  public PackKafkaReader(String name, String serialId, PackKafkaClientFactory kafkaClientFactory,
      WalCacheManager walCacheManager, MaxBlockLayer maxBlockLayer, String topic, Integer partition) {
    super(name, walCacheManager);
    _syncDelay = TimeUnit.MILLISECONDS.toMillis(10);
    _serialId = serialId;
    _topic = topic;
    _partition = partition;
    _kafkaClientFactory = kafkaClientFactory;
    _maxBlockLayer = maxBlockLayer;
    _endPointLookup = new EndPointLookup(name, kafkaClientFactory, _serialId, topic, partition);
    _kafkaPollTimeout = TimeUnit.MILLISECONDS.toMillis(10);
  }

  public void sync() throws IOException {
    long endpoint = _endPointLookup.getEndpoint();
    while (endpoint + 1 < _endOffset.get()) {
      LOGGER.info("Waiting for sync {}", _topic);
      try {
        Thread.sleep(_syncDelay);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  protected void writeDataToWal(WalCacheManager walCacheManager) throws IOException {
    try (KafkaConsumer<byte[], Blocks> consumer = _kafkaClientFactory.createConsumer(_serialId)) {
      TopicPartition partition = new TopicPartition(_topic, _partition);
      long maxLayer = _maxBlockLayer.getMaxLayer();
      ImmutableList<TopicPartition> partitions = ImmutableList.of(partition);
      consumer.assign(partitions);
      consumer.seek(partition, maxLayer);
      setEndOffset(consumer, partition, partitions);
      while (_running.get()) {
        ConsumerRecords<byte[], Blocks> records = consumer.poll(_kafkaPollTimeout);
        for (ConsumerRecord<byte[], Blocks> record : records) {
          Blocks blocks = record.value();
          for (Block block : blocks.getBlocks()) {
            walCacheManager.write(block.getTransId(), record.offset(), block.getBlockId(), block.getData());
          }
        }
        setEndOffset(consumer, partition, partitions);
      }
    }
  }

  private void setEndOffset(KafkaConsumer<byte[], Blocks> consumer, TopicPartition partition,
      ImmutableList<TopicPartition> partitions) {
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
    Long endOffset = endOffsets.get(partition);
    if (endOffset == null) {
      _endOffset.set(Long.MAX_VALUE);
    } else {
      _endOffset.set(endOffset);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    PackUtils.closeQuietly(_endPointLookup);
  }

}
