package pack.distributed.storage.kafka;

import java.io.File;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import pack.distributed.storage.kafka.util.LookupKafkaBrokers;

public class PackKafkaClientFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackKafkaClientFactory.class);

  private static final String KAFKA = "kafka.";
  private static final String FALSE = "false";
  private static final String ALL = "all";

  private final List<String> _bootstrapServers;
  private final Properties _extraKafkaProps;

  public PackKafkaClientFactory(String zkConnection) {
    this(LookupKafkaBrokers.getAllBrokers(zkConnection));
  }

  public PackKafkaClientFactory(File kafkaConfig) {
    this(LookupKafkaBrokers.getAllBrokers(kafkaConfig));
  }

  public PackKafkaClientFactory(List<String> bootstrapServers) {
    _bootstrapServers = bootstrapServers;
    _extraKafkaProps = getExtraKafkaProps();
  }

  public KafkaProducer<Integer, byte[]> createProducer() {
    Properties props = new Properties();
    props.putAll(_extraKafkaProps);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, toString(_bootstrapServers));
    props.put(ProducerConfig.ACKS_CONFIG, ALL);
    props.put(ProducerConfig.RETRIES_CONFIG, 1_000_000_000);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1_048_576);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  public KafkaConsumer<Integer, byte[]> createConsumer(String groupId) {
    Properties props = new Properties();
    props.putAll(_extraKafkaProps);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, toString(_bootstrapServers));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }

  public AdminClient createAdmin() {
    Properties props = new Properties();
    props.putAll(_extraKafkaProps);
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, toString(_bootstrapServers));
    return AdminClient.create(props);
  }

  private Properties getExtraKafkaProps() {
    Properties extraKafkaProps = new Properties();
    Properties properties = System.getProperties();
    Enumeration<?> propertyNames = properties.propertyNames();
    while (propertyNames.hasMoreElements()) {
      Object propName = propertyNames.nextElement();
      if (propName == null) {
        continue;
      }
      String prop = propName.toString();
      String value = properties.getProperty(prop);
      if (prop.startsWith(KAFKA)) {
        String key = prop.substring(KAFKA.length());
        LOGGER.info("extra kafak prop {} => {}", key, value);
        extraKafkaProps.put(key, value);
      }
    }
    return extraKafkaProps;
  }

  private static String toString(List<String> bootstrapServers) {
    return Joiner.on(',')
                 .join(bootstrapServers);
  }
}
