package pack.iscsi.storage.kafka;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import com.google.common.collect.ImmutableList;

public class PackKafkaManager {

  private static final String FALSE = "false";
  private static final String ALL = "all";

  private final String _bootstrapServers;
  private final String _groupId;

  public PackKafkaManager(String bootstrapServers, String groupId) {
    _bootstrapServers = bootstrapServers;
    _groupId = groupId;
  }

  public KafkaProducer<Long, byte[]> createProducer(String kafkaTopic) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
    props.put(ProducerConfig.ACKS_CONFIG, ALL);
    props.put(ProducerConfig.RETRIES_CONFIG, 1_000_000_000);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1_048_576);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  public KafkaConsumer<Long, byte[]> createConsumer(String kafkaTopic) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, _groupId);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }

  public void createTopicIfMissing(String kafkaTopic) throws InterruptedException, ExecutionException {
    Properties props = new Properties();
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
    try (AdminClient adminClient = AdminClient.create(props)) {
      Set<String> names = adminClient.listTopics()
                                     .names()
                                     .get();
      if (!names.contains(kafkaTopic)) {
        adminClient.createTopics(ImmutableList.of(new NewTopic(kafkaTopic, 1, (short) 3)));
      }
    }
  }

  public void deleteTopic(String kafkaTopic) throws InterruptedException, ExecutionException {
    Properties props = new Properties();
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
    try (AdminClient adminClient = AdminClient.create(props)) {
      Set<String> names = adminClient.listTopics()
                                     .names()
                                     .get();
      if (names.contains(kafkaTopic)) {
        adminClient.deleteTopics(ImmutableList.of(kafkaTopic));
      }
    }
  }

}
