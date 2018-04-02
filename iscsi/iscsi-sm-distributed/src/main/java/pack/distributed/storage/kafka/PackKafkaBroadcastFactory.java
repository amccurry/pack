package pack.distributed.storage.kafka;

import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.broadcast.PackBroadcastFactory;
import pack.distributed.storage.broadcast.PackBroadcastReader;
import pack.distributed.storage.broadcast.PackBroadcastWriter;
import pack.distributed.storage.hdfs.MaxBlockLayer;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.ServerStatusManager;
import pack.distributed.storage.wal.WalCacheManager;

public class PackKafkaBroadcastFactory extends PackBroadcastFactory {

  private final Integer _topicPartition = 0;
  private final PackKafkaClientFactory _kafkaClientFactory;

  public PackKafkaBroadcastFactory(PackKafkaClientFactory kafkaClientFactory) {
    _kafkaClientFactory = kafkaClientFactory;
  }

  public PackBroadcastWriter createPackBroadcastWriter(String name, PackMetaData metaData,
      WriteBlockMonitor writeBlockMonitor, ServerStatusManager serverStatusManager) {
    return new PackKafkaWriter(name, _kafkaClientFactory.createProducer(), metaData.getTopicId(), _topicPartition,
        writeBlockMonitor, serverStatusManager);
  }

  public PackBroadcastReader createPackBroadcastReader(String name, PackMetaData metaData,
      WalCacheManager walCacheManager, MaxBlockLayer maxBlockLayer) {
    return new PackKafkaReader(name, metaData.getSerialId(), _kafkaClientFactory, walCacheManager, maxBlockLayer,
        metaData.getTopicId(), _topicPartition);
  }

}
