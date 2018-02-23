package pack.iscsi.storage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TestStorageModuleConfig {
  FileSystem fileSystem;
  Path packRoot;
  String id;
  long sizeInBytes;
  KafkaProducer<byte[], byte[]> producer;
  KafkaConsumer<byte[], byte[]> consumer;
  String topic;
}
