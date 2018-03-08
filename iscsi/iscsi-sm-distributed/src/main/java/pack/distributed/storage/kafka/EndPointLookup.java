package pack.distributed.storage.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class EndPointLookup implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(EndPointLookup.class);

  private final AtomicLong _endpoint = new AtomicLong();
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final Thread _thread;
  private final Object _lock = new Object();

  public EndPointLookup(String name, PackKafkaClientFactory clientFactory, String serialId, String topic,
      Integer partition) {
    _thread = new Thread(new Runnable() {
      @Override
      public void run() {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        while (_running.get()) {
          try {
            KafkaConsumer<Integer, byte[]> consumer = clientFactory.createConsumer(serialId);
            while (_running.get()) {
              synchronized (_lock) {
                lookupEndpoint(consumer, topicPartition);
                try {
                  _lock.notify();
                  _lock.wait();
                } catch (InterruptedException e) {
                  LOGGER.error("Unknown error", e);
                }
              }
            }
          } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
            if (!_running.get()) {
              return;
            }
          }
        }
      }
    });
    _thread.setDaemon(true);
    _thread.setName("endpoint-check-name");
    _thread.start();
  }

  protected void lookupEndpoint(KafkaConsumer<Integer, byte[]> consumer, TopicPartition partition) {
    Map<TopicPartition, Long> offsets = consumer.endOffsets(ImmutableList.of(partition));
    Long offset = offsets.get(partition);
    if (offset == null) {
      _endpoint.set(Long.MAX_VALUE);
    } else {
      _endpoint.set(offset);
    }
  }

  public long getEndpoint() throws IOException {
    synchronized (_lock) {
      try {
        _lock.notify();
        _lock.wait();
      } catch (InterruptedException e) {
        LOGGER.error("Unknown error", e);
        throw new IOException(e);
      }
      return _endpoint.get();
    }
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    _thread.interrupt();
  }

}
