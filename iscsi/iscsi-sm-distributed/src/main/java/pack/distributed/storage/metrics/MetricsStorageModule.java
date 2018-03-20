package pack.distributed.storage.metrics;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

public class MetricsStorageModule implements IStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsStorageModule.class);

  private final IStorageModule _delegate;
  private final MetricRegistry _registry;
  private final Timer _writeTimer;
  private final Timer _readTimer;
  private final Timer _flushTimer;
  private final Meter _writerMeter;
  private final Meter _readMeter;
  private final Meter _readIops;
  private final Meter _writeIops;

  public static IStorageModule wrap(String name, MetricRegistry registry, IStorageModule module) {
    LOGGER.info("Adding metrics to {} {}", name, module);
    return new MetricsStorageModule(name, registry, module);
  }

  public MetricsStorageModule(String name, MetricRegistry registry, IStorageModule delegate) {
    _registry = registry;
    _delegate = delegate;
    _writeTimer = _registry.timer(name + "." + "write.latency");
    _readTimer = _registry.timer(name + "." + "read.latency");
    _flushTimer = _registry.timer(name + "." + "flush.latency");
    _writerMeter = _registry.meter(name + "." + "write.throughput");
    _readMeter = _registry.meter(name + "." + "read.throughput");
    _readIops = _registry.meter(name + "." + "read.iops");
    _writeIops = _registry.meter(name + "." + "write.iops");
  }

  @Override
  public int checkBounds(long logicalBlockAddress, int transferLengthInBlocks) {
    return _delegate.checkBounds(logicalBlockAddress, transferLengthInBlocks);
  }

  @Override
  public long getSizeInBlocks() {
    return _delegate.getSizeInBlocks();
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    try (Context context = _readTimer.time()) {
      _delegate.read(bytes, storageIndex);
    }
    _readMeter.mark(bytes.length);
    _readIops.mark();
  }

  @Override
  public void read(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber) throws IOException {
    try (Context context = _readTimer.time()) {
      _delegate.read(bytes, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber);
    }
    _readMeter.mark(bytes.length);
    _readIops.mark();
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    try (Context context = _writeTimer.time()) {
      _delegate.write(bytes, storageIndex);
    }
    _writerMeter.mark(bytes.length);
    _writeIops.mark();
  }

  @Override
  public void write(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber, Integer dataSequenceNumber, Integer targetTransferTag) throws IOException {
    try (Context context = _writeTimer.time()) {
      _delegate.write(bytes, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber, dataSequenceNumber,
          targetTransferTag);
    }
    _writerMeter.mark(bytes.length);
    _writeIops.mark();
  }

  @Override
  public void close() throws IOException {
    _delegate.close();
  }

  @Override
  public void flushWrites() throws IOException {
    try (Context context = _flushTimer.time()) {
      _delegate.flushWrites();
    }
  }

  @Override
  public int getBlockSize() {
    return _delegate.getBlockSize();
  }

  @Override
  public UUID getSerialId() {
    return _delegate.getSerialId();
  }

}
