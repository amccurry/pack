package pack.distributed.storage.kafka;

import java.io.Closeable;
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

import pack.distributed.storage.hdfs.PackHdfsReader;
import pack.distributed.storage.kafka.util.HeaderUtil;
import pack.distributed.storage.wal.WalCacheManager;
import pack.iscsi.storage.utils.PackUtils;

public class PackKafkaReader implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackKafkaReader.class);

  private final PackKafkaClientFactory _kafkaClientFactory;
  private final WalCacheManager _walCacheManager;
  private final PackHdfsReader _hdfsReader;
  private final String _name;
  private final Thread _kafkaReader;
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final String _serialId;
  private final String _topic;
  private final Integer _partition;
  private final AtomicLong _endOffset = new AtomicLong(Long.MAX_VALUE);
  private final EndPointLookup _endPointLookup;
  private final long _kafkaPollTimeout;

  public PackKafkaReader(String name, String serialId, PackKafkaClientFactory kafkaClientFactory,
      WalCacheManager walCacheManager, PackHdfsReader hdfsReader, String topic, Integer partition) {
    _name = name;
    _serialId = serialId;
    _topic = topic;
    _partition = partition;
    _kafkaClientFactory = kafkaClientFactory;
    _walCacheManager = walCacheManager;
    _hdfsReader = hdfsReader;
    _endPointLookup = new EndPointLookup(name, kafkaClientFactory, serialId, topic, partition);
    _kafkaPollTimeout = TimeUnit.MILLISECONDS.toMillis(10);
    _kafkaReader = new Thread(() -> {
      while (_running.get()) {
        try {
          writeDataToWal();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
      }
    });
    _kafkaReader.setDaemon(true);
    _kafkaReader.setName("PackKafkaReader-" + _name);

  }

  private void writeDataToWal() throws IOException {
    try (KafkaConsumer<Integer, byte[]> consumer = _kafkaClientFactory.createConsumer(_serialId)) {
      TopicPartition partition = new TopicPartition(_topic, _partition);
      long maxLayer = _hdfsReader.getMaxLayer();
      ImmutableList<TopicPartition> partitions = ImmutableList.of(partition);
      consumer.assign(partitions);
      consumer.seek(partition, maxLayer);
      setEndOffset(consumer, partition, partitions);
      while (_running.get()) {
        ConsumerRecords<Integer, byte[]> records = consumer.poll(_kafkaPollTimeout);
        for (ConsumerRecord<Integer, byte[]> record : records) {
          Integer blockId = record.key();
          byte[] value = record.value();
          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("wal consumer blockId {} md5 {}", blockId, PackUtils.toMd5(value));
          }
          long transId = HeaderUtil.getTransId(record.headers());
          _walCacheManager.write(transId, record.offset(), blockId, value);
        }
        setEndOffset(consumer, partition, partitions);
      }
    }
  }

  private void setEndOffset(KafkaConsumer<Integer, byte[]> consumer, TopicPartition partition,
      ImmutableList<TopicPartition> partitions) {
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
    Long endOffset = endOffsets.get(partition);
    if (endOffset == null) {
      _endOffset.set(Long.MAX_VALUE);
    } else {
      _endOffset.set(endOffset);
    }
  }

  public void start() {
    _kafkaReader.start();
  }

  @Override
  public void close() throws IOException {
    PackUtils.closeQuietly(_endPointLookup);
    _running.set(false);
    _kafkaReader.interrupt();
  }

}
