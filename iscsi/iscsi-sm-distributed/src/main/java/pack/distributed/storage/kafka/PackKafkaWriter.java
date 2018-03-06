package pack.distributed.storage.kafka;

import java.io.Closeable;
import java.io.IOException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PackKafkaWriter implements Closeable {

  private final Producer<Integer, byte[]> _producer;
  private final String _topic;
  private final Integer _partition;

  public PackKafkaWriter(Producer<Integer, byte[]> producer, String topic, Integer partition) {
    _producer = producer;
    _topic = topic;
    _partition = partition;
  }

  public void write(int blockId, byte[] bs, int off, int len) {
    byte[] value = new byte[len];
    System.arraycopy(bs, off, value, 0, len);
    _producer.send(new ProducerRecord<Integer, byte[]>(_topic, _partition, blockId, value));
  }

  public void flush() {
    _producer.flush();
  }

  @Override
  public void close() throws IOException {
    _producer.close();
  }

}
