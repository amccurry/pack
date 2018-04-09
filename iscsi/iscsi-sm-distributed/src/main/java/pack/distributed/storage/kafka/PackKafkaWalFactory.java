package pack.distributed.storage.kafka;

import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.MaxBlockLayer;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.BroadcastServerManager;
import pack.distributed.storage.wal.PackWalFactory;
import pack.distributed.storage.wal.PackWalReader;
import pack.distributed.storage.wal.PackWalWriter;
import pack.distributed.storage.walcache.WalCacheManager;

public class PackKafkaWalFactory extends PackWalFactory {

  private final Integer _topicPartition = 0;
  private final PackKafkaClientFactory _kafkaClientFactory;

  public PackKafkaWalFactory(PackKafkaClientFactory kafkaClientFactory) {
    _kafkaClientFactory = kafkaClientFactory;
  }

  public PackWalWriter createPackWalWriter(String name, PackMetaData metaData,
      WriteBlockMonitor writeBlockMonitor, BroadcastServerManager serverStatusManager) {
    return new PackKafkaWriter(name, _kafkaClientFactory.createProducer(), metaData.getTopicId(), _topicPartition,
        writeBlockMonitor, serverStatusManager);
  }

  public PackWalReader createPackWalReader(String name, PackMetaData metaData,
      WalCacheManager walCacheManager, MaxBlockLayer maxBlockLayer) {
    return new PackKafkaReader(name, metaData.getSerialId(), _kafkaClientFactory, walCacheManager, maxBlockLayer,
        metaData.getTopicId(), _topicPartition);
  }

}
