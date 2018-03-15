package pack.distributed.storage.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import pack.distributed.storage.kafka.util.HeaderUtil;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.ServerStatusManager;
import pack.distributed.storage.status.UpdateBlockId;
import pack.distributed.storage.status.UpdateBlockIdBatch;
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
  private final List<UpdateBlockId> _updateBlockBatch = new ArrayList<>();

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
      long transId = _writeBlockMonitor.createTransId();
      _writeBlockMonitor.addDirtyBlock(blockId, transId);

      UpdateBlockId updateBlockId = UpdateBlockId.builder()
                                                 .blockId(blockId)
                                                 .transId(transId)
                                                 .build();
      _updateBlockBatch.add(updateBlockId);
      _producer.send(new ProducerRecord<Integer, byte[]>(_topic, _partition, blockId, value, getHeaders(transId)));
    }
  }

  private Iterable<Header> getHeaders(long transId) {
    return Arrays.asList(HeaderUtil.toTransHeader(transId));
  }

  public void flush(PackTracer tracer) {
    try (PackTracer span = tracer.span(LOGGER, "producer flush")) {
      _producer.flush();
      UpdateBlockIdBatch batch = UpdateBlockIdBatch.builder()
                                                   .batch(ImmutableList.copyOf(_updateBlockBatch))
                                                   .volume(_volumeName)
                                                   .build();
      _updateBlockBatch.clear();
      _serverStatusManager.broadcastToAllServers(batch);
    }
  }

  @Override
  public void close() throws IOException {
    _producer.close();
  }

}
