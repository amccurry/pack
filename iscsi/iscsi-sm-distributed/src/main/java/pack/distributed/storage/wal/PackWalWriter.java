package pack.distributed.storage.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.ImmutableList;

import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.BlockUpdateInfo;
import pack.distributed.storage.status.BlockUpdateInfoBatch;
import pack.distributed.storage.status.BroadcastServerManager;
import pack.distributed.storage.trace.PackTracer;
import pack.iscsi.metrics.MetricsRegistrySingleton;
import pack.iscsi.storage.utils.PackUtils;

public abstract class PackWalWriter implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackWalWriter.class);
  private final WriteBlockMonitor _writeBlockMonitor;
  private final BroadcastServerManager _serverStatusManager;
  private final String _volumeName;

  private final Timer _kafkaFlush;
  private final Timer _broadcast;
  private final List<Block> _blockBatch = new ArrayList<>();
  private final List<BlockUpdateInfo> _transBlockInfoBatch = new ArrayList<>();

  private final Object _writeLock = new Object();

  public PackWalWriter(String volumeName, WriteBlockMonitor writeBlockMonitor,
      BroadcastServerManager serverStatusManager) {
    _volumeName = volumeName;
    _serverStatusManager = serverStatusManager;
    _writeBlockMonitor = writeBlockMonitor;
    MetricRegistry registry = MetricsRegistrySingleton.getInstance();
    _kafkaFlush = registry.timer(volumeName + ".kafka.flush");
    _broadcast = registry.timer(volumeName + ".pack.broadcast");
  }

  public void write(PackTracer tracer, int blockId, byte[] bs, int off, int len) {
    synchronized (_writeLock) {
      long transId = _writeBlockMonitor.createTransId();

      _writeBlockMonitor.addDirtyBlock(blockId, transId);

      _blockBatch.add(Block.builder()
                           .blockId(blockId)
                           .transId(transId)
                           .data(PackUtils.copy(bs, off, len))
                           .build());

      _transBlockInfoBatch.add(BlockUpdateInfo.builder()
                                              .blockId(blockId)
                                              .transId(transId)
                                              .build());
    }
  }

  private void writeBlocks(PackTracer tracer) throws IOException {
    try (PackTracer span = tracer.span(LOGGER, "producer send message")) {
      if (_blockBatch.isEmpty()) {
        return;
      }
      Blocks blocks = Blocks.builder()
                            .blocks(ImmutableList.copyOf(_blockBatch))
                            .build();
      _blockBatch.clear();
      writeBlocks(blocks);
    }
  }

  public void flush(PackTracer tracer) throws IOException, InterruptedException {
    writeBlocks(tracer);
    try (Context time = _kafkaFlush.time()) {
      internalFlush();
    }
    try (Context time = _broadcast.time()) {
      notifyOtherServers();
    }
  }

  private void notifyOtherServers() throws InterruptedException {
    BlockUpdateInfoBatch batch = BlockUpdateInfoBatch.builder()
                                                     .batch(ImmutableList.copyOf(_transBlockInfoBatch))
                                                     .volume(_volumeName)
                                                     .build();
    _transBlockInfoBatch.clear();
    _serverStatusManager.broadcastToAllServers(batch);
  }

  @Override
  public void close() throws IOException {
    PackUtils.close(LOGGER, () -> internalClose());
  }

  protected abstract void writeBlocks(Blocks blocks) throws IOException;

  protected abstract void internalFlush() throws IOException;

  protected abstract void internalClose() throws IOException;

}
