package pack.distributed.storage.monitor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import pack.distributed.storage.hdfs.PackHdfsReader;
import pack.distributed.storage.hdfs.ReadRequest;
import pack.distributed.storage.kafka.EndPointLookup;
import pack.distributed.storage.kafka.PackKafkaClientFactory;
import pack.distributed.storage.trace.PackTracer;
import pack.distributed.storage.wal.WalCacheManager;

public class PackWriteMonitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(EndPointLookup.class);

  private final AtomicReference<Value> _value = new AtomicReference<PackWriteMonitor.Value>();
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final Thread _thread;
  private final PackHdfsReader _hdfsReader;
  private final WalCacheManager _walCacheManager;
  private final long _pollDelay;
  private final long _allowedOverlap;

  static class Value {
    final long _endOffset;
    final long _ts;

    Value(long endpoint, long ts) {
      _endOffset = endpoint;
      _ts = ts;
    }
  }

  public PackWriteMonitor(String name, PackHdfsReader hdfsReader, WalCacheManager walCacheManager,
      PackKafkaClientFactory clientFactory, String serialId, String topic, Integer partition) {
    _hdfsReader = hdfsReader;
    _walCacheManager = walCacheManager;
    _pollDelay = TimeUnit.MILLISECONDS.toMillis(1);
    _allowedOverlap = TimeUnit.MILLISECONDS.toMillis(7);
    _thread = new Thread(() -> {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      while (_running.get()) {
        try (KafkaConsumer<Integer, byte[]> consumer = clientFactory.createConsumer(serialId)) {
          while (_running.get()) {
            lookupEndpoint(consumer, topicPartition);
            Thread.sleep(_pollDelay);
          }
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          if (!_running.get()) {
            return;
          }
        }
      }
    });
    _thread.setDaemon(true);
    _thread.setName("write-monitor");
    _thread.start();
  }

  protected void lookupEndpoint(KafkaConsumer<Integer, byte[]> consumer, TopicPartition partition) {
    Map<TopicPartition, Long> offsets = consumer.endOffsets(ImmutableList.of(partition));
    Long offset = offsets.get(partition);
    if (offset == null) {
      _value.set(new Value(Long.MAX_VALUE, System.nanoTime()));
    } else {
      _value.set(new Value(offset, System.nanoTime()));
    }
  }

  public void waitForDataIfNeeded(PackTracer tracer, List<ReadRequest> requests, WriteBlockMonitor writeBlockMonitor)
      throws IOException {
    try (PackTracer span = tracer.span(LOGGER, "waitForDataIfNeeded")) {
      // long now = System.nanoTime();
      for (ReadRequest readRequest : requests) {
        writeBlockMonitor.waitIfNeededForSync(readRequest.getBlockId());
        // if (writeBlockMonitor.isBlockDirty(readRequest.getBlockId(),
        // System.currentTimeMillis())) {
        // waitForWallToSync(tracer, now);
        // return;
        // }
      }
    }
  }
  //
  // private void waitForWallToSync(PackTracer tracer, long now) throws
  // IOException {
  // try (PackTracer span = tracer.span(LOGGER, "waitForWallToSync")) {
  // while (shouldWaitForSync(now)) {
  // try {
  // Thread.sleep(3);
  // } catch (InterruptedException e) {
  // throw new IOException(e);
  // }
  // }
  // }
  // }

  // private boolean shouldWaitForSync(long now) throws IOException {
  // Value ep = getEndPoint();
  // if (!isUpToDate(ep, now)) {
  // return true;
  // }
  // long endOffset = ep._endOffset;
  // long hdfsMaxLayer = _hdfsReader.getMaxLayer();
  // long walMaxLayer = _walCacheManager.getMaxLayer();
  // if (LOGGER.isTraceEnabled()) {
  // LOGGER.trace("shouldWaitForSync endOffset {} hdfs {} wal {}", endOffset,
  // hdfsMaxLayer, walMaxLayer);
  // }
  // if (endOffset == Long.MAX_VALUE) {
  // // EndOffset is not know always wait
  // return true;
  // } else if (walMaxLayer != -1L && walMaxLayer + 1 >= endOffset) {
  // // else if walMaxLayer has recved all data upto endOffset don't wait
  // return false;
  // } else if (hdfsMaxLayer == endOffset) {
  // // else if hdfs is update to date don't wait
  // return false;
  // } else {
  // // otherwise we have to wait for this node to catch up
  // return true;
  // }
  // }

  // private boolean isUpToDate(Value ep, long now) {
  // if (ep._ts + _allowedOverlap < now) {
  // return false;
  // }
  // return true;
  // }
  //
  // private Value getEndPoint() {
  // return _value.get();
  // }
}
