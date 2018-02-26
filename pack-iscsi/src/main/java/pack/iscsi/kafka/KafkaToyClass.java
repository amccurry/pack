package pack.iscsi.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.google.common.collect.ImmutableList;

public class KafkaToyClass {

  public static void main(String[] args) {
    String bootstrapServers = "centos-01:9092,centos-02:9092,centos-03:9092,centos-04:9092";
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.RETRIES_CONFIG, 1_000_000_000);
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    String topic = "testing";
    createTopicIfMissing(topic, bootstrapServers);

    Properties props = new Properties();

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    AtomicLong endOffset = new AtomicLong();
    Thread thread = new Thread(() -> {
      try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
        while (true) {
          TopicPartition topicPartition = new TopicPartition(topic, 0);
          Map<TopicPartition, Long> endOffsets = consumer.endOffsets(ImmutableList.of(topicPartition));
          endOffset.set(endOffsets.get(topicPartition));
        }
      }
    });
    thread.setDaemon(true);
    thread.start();

    AtomicLong lastConfirmedOffset = new AtomicLong();
    Callback callback = (metadata, exception) -> lastConfirmedOffset.set(metadata.offset());
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
      // producer.initTransactions();
      long totalTime = 0;
      long count = 0;
      long start = System.nanoTime();

      int batchCount = 0;
      for (int i = 0; i < 1_000_000; i++) {
        if (start + TimeUnit.SECONDS.toNanos(3) < System.nanoTime()) {
          System.out.println(
              "count [" + count + "] lastoffset [" + lastConfirmedOffset.get() + "] endOffset [" + endOffset.get()
                  + "] totalTime [" + totalTime + "] avg [" + ((double) totalTime / count) / 1_000_000.0 + " ms]");
          start = System.nanoTime();
        }
        long s = System.nanoTime();
        // producer.beginTransaction();
        Long timestamp = System.currentTimeMillis();
        producer.send(new ProducerRecord<byte[], byte[]>(topic, 0, timestamp, getKey(), getValue()), callback);
        batchCount++;
        if (batchCount == 10) {
          producer.flush();
          batchCount = 0;
        }
        // producer.commitTransaction();
        long e = System.nanoTime();
        totalTime += (e - s);
        count++;
      }
    }
  }

  private static byte[] getKey() {
    return new byte[8];
  }

  private static byte[] getValue() {
    return new byte[8192];
  }

  private static void createTopicIfMissing(String topic, String bootstrapServers) {
    Properties props = new Properties();
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    try (AdminClient adminClient = AdminClient.create(props)) {
      CreateTopicsOptions options = new CreateTopicsOptions();
      adminClient.deleteTopics(ImmutableList.of(topic));
      adminClient.createTopics(ImmutableList.of(new NewTopic(topic, 1, (short) 3)), options);
    }
  }
}
