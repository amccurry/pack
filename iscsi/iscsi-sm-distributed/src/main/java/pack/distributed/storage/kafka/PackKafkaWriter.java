package pack.distributed.storage.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.kafka.util.HeaderUtil;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.ServerStatusManager;
import pack.distributed.storage.trace.PackTracer;
import pack.iscsi.storage.utils.PackUtils;

public class PackKafkaWriter implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackKafkaWriter.class);
  private final Producer<Integer, byte[]> _producer;
  private final String _topic;
  private final Integer _partition;
  private final WriteBlockMonitor _writeBlockMonitor;
  private final ServerStatusManager _serverStatusManager;
  private final String _volumeName;

  public PackKafkaWriter(String volumeName, Producer<Integer, byte[]> producer, String topic, Integer partition,
      WriteBlockMonitor writeBlockMonitor, ServerStatusManager serverStatusManager) {
    _volumeName = volumeName;
    _serverStatusManager = serverStatusManager;
    _writeBlockMonitor = writeBlockMonitor;
    _producer = producer;
    _topic = topic;
    _partition = partition;
  }

  public void write(PackTracer tracer, int blockId, byte[] bs, int off, int len) {
    byte[] value = new byte[len];
    System.arraycopy(bs, off, value, 0, len);
    try (PackTracer span = tracer.span(LOGGER, "producer send")) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("write blockId {} md5 {}", blockId, PackUtils.toMd5(value));
      }
      // _producer.send(new ProducerRecord<Integer, byte[]>(_topic, _partition,
      // blockId, value),
      // (metadata, exception) -> handleCallback(blockId, metadata));
      long transId = _writeBlockMonitor.createTransId();
      _writeBlockMonitor.addDirtyBlock(blockId, transId);
      _serverStatusManager.broadcastToAllServers(_volumeName, blockId, transId);
      _producer.send(new ProducerRecord<Integer, byte[]>(_topic, _partition, blockId, value, getHeaders(transId)));
    }
  }

  private Iterable<Header> getHeaders(long transId) {
    return Arrays.asList(HeaderUtil.toTransHeader(transId));
  }

  //
  // protected void handleCallback(int blockId, RecordMetadata metadata) {
  // long offset = metadata.offset();
  // _writeBlockMonitor.addDirtyBlock(blockId, offset);
  // _serverStatusManager.broadcastToAllServers(_volumeName, blockId, offset);
  // }

  public void flush(PackTracer tracer) {
    try (PackTracer span = tracer.span(LOGGER, "producer flush")) {
      _producer.flush();
    }
  }

  @Override
  public void close() throws IOException {
    _producer.close();
  }

}
