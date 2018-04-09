package pack.distributed.storage.kafka;

import java.io.IOException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.BroadcastServerManager;
import pack.distributed.storage.wal.Blocks;
import pack.distributed.storage.wal.PackWalWriter;

public class PackKafkaWriter extends PackWalWriter {

  private final Producer<byte[], Blocks> _producer;
  private final String _topic;
  private final Integer _partition;

  public PackKafkaWriter(String volumeName, Producer<byte[], Blocks> producer, String topic, Integer partition,
      WriteBlockMonitor writeBlockMonitor, BroadcastServerManager serverStatusManager) {
    super(volumeName, writeBlockMonitor, serverStatusManager);
    _producer = producer;
    _topic = topic;
    _partition = partition;
  }

  @Override
  protected void writeBlocks(Blocks blocks) throws IOException {
    _producer.send(new ProducerRecord<byte[], Blocks>(_topic, _partition, null, blocks));
  }

  @Override
  protected void internalFlush() throws IOException {
    _producer.flush();
  }

  @Override
  protected void internalClose() throws IOException {
    _producer.close();
  }

}
