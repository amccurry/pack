package pack.iscsi.metrics;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import org.jscsi.target.storage.IStorageModule;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

public class MetricsIStorageModule implements IStorageModule {

  private final IStorageModule _delegate;
  private final MetricRegistry _registry;
  private final Timer _writeTimer;
  private final Timer _readTimer;
  private final Timer _flushTimer;
  private final Histogram _writerHistogram;
  private final Histogram _readHistogram;

  public static IStorageModule wrap(String name, MetricRegistry registry, IStorageModule module) {
    return new MetricsIStorageModule(name, registry, module);
  }

  public MetricsIStorageModule(String name, MetricRegistry registry, IStorageModule delegate) {
    _registry = registry;
    _delegate = delegate;
    _writeTimer = _registry.timer(name + "." + "write.latency");
    _readTimer = _registry.timer(name + "." + "read.latency");
    _flushTimer = _registry.timer(name + "." + "flush.latency");
    _writerHistogram = _registry.histogram(name + "." + "write.throughput");
    _readHistogram = _registry.histogram(name + "." + "read.throughput");
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
    _readHistogram.update(bytes.length);
  }

  @Override
  public void read(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber) throws IOException {
    try (Context context = _readTimer.time()) {
      _delegate.read(bytes, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber);
    }
    _readHistogram.update(bytes.length);
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    try (Context context = _writeTimer.time()) {
      _delegate.write(bytes, storageIndex);
    }
    _writerHistogram.update(bytes.length);
  }

  @Override
  public void write(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber, Integer dataSequenceNumber, Integer targetTransferTag) throws IOException {
    try (Context context = _writeTimer.time()) {
      _delegate.write(bytes, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber, dataSequenceNumber,
          targetTransferTag);
    }
    _writerHistogram.update(bytes.length);
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
