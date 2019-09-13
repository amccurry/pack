package pack.iscsi.volume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.LoadingCache;

import io.opencensus.common.Scope;
import pack.iscsi.block.AlreadyClosedException;
import pack.iscsi.block.Block;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.wal.BlockWriteAheadLogResult;
import pack.iscsi.util.Utils;
import pack.util.TracerUtil;

public class BlockStorageModule implements StorageModule {

  private static final String WRITE = "write";

  private static final String READ = "read";

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModule.class);

  private final long _volumeId;
  private final int _blockSize;
  private final long _lengthInBytes;
  private final LoadingCache<BlockKey, Block> _cache;
  private final AtomicBoolean _closed = new AtomicBoolean();
  private final BlockIOFactory _externalBlockStoreFactory;
  private final Timer _syncTimer;
  private final ExecutorService _syncExecutor;
  private final MetricRegistry _metrics;
  private final Meter _readMeter;
  private final Meter _writeMeter;

  public BlockStorageModule(BlockStorageModuleConfig config) {
    _metrics = config.getMetrics();
    String volumeName = config.getVolumeName();
    _readMeter = _metrics.meter(MetricRegistry.name(BlockStorageModule.class, volumeName, READ));
    _writeMeter = _metrics.meter(MetricRegistry.name(BlockStorageModule.class, volumeName, WRITE));

    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();

    _volumeId = config.getVolumeId();
    _blockSize = config.getBlockSize();
    _lengthInBytes = config.getLengthInBytes();
    _cache = config.getGlobalCache();
    _syncExecutor = config.getSyncExecutor();
    _syncTimer = new Timer("sync-" + _volumeId);

    long period = config.getSyncTimeAfterIdleTimeUnit()
                        .toMillis(config.getSyncTimeAfterIdle());
    _syncTimer.schedule(getTask(), period, period);
  }

  @Override
  public void read(byte[] bytes, long position) throws IOException {
    checkClosed();
    int length = bytes.length;
    _readMeter.mark(length);
    int offset = 0;
    try (Scope readScope = TracerUtil.trace(READ)) {
      while (length > 0) {
        long blockId = getBlockId(position);
        int blockOffset = getBlockOffset(position);
        int remaining = _blockSize - blockOffset;
        int len = Math.min(remaining, length);
        BlockKey blockKey = BlockKey.builder()
                                    .volumeId(_volumeId)
                                    .blockId(blockId)
                                    .build();
        Block block = getBlockId(blockKey);

        try (Scope blockWriterScope = TracerUtil.trace("block read")) {
          block.readFully(blockOffset, bytes, offset, len);
        }
        length -= len;
        position += len;
        offset += len;
      }
    }
  }

  private List<BlockWriteAheadLogResult> _results = new ArrayList<>();
  private final AtomicLong _writesCount = new AtomicLong();

  @Override
  public void write(byte[] bytes, long position) throws IOException {
    checkClosed();
    _writesCount.addAndGet(bytes.length);
    int length = bytes.length;
    _writeMeter.mark(length);
    int offset = 0;
    try (Scope writeScope = TracerUtil.trace(WRITE)) {
      while (length > 0) {
        long blockId = getBlockId(position);
        int blockOffset = getBlockOffset(position);
        int remaining = _blockSize - blockOffset;
        int len = Math.min(remaining, length);
        BlockKey blockKey = BlockKey.builder()
                                    .volumeId(_volumeId)
                                    .blockId(blockId)
                                    .build();
        Block block = getBlockId(blockKey);

        // @TODO perhaps we should do something with the result
        try (Scope blockWriterScope = TracerUtil.trace("block write")) {
          trackResult(block.writeFully(blockOffset, bytes, offset, len));
        }
        length -= len;
        position += len;
        offset += len;
      }
    }
  }

  @Override
  public void flushWrites() throws IOException {
    int size = _results.size();
    long writeCount = _writesCount.getAndSet(0);
    long start = System.nanoTime();
    try (Scope writeScope = TracerUtil.trace("flushWrites")) {
      for (BlockWriteAheadLogResult result : _results) {
        result.get();
      }
      _results.clear();
    }
    long end = System.nanoTime();
    LOGGER.info("flushWrites {} {} in {} ms", size, writeCount, (end - start) / 1_000_000.0);
  }

  private void trackResult(BlockWriteAheadLogResult result) {
    _results.add(result);
  }

  private Block getBlockId(BlockKey blockKey) {
    try (Scope scope = TracerUtil.trace("get block")) {
      return _cache.get(blockKey);
    }
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("starting close of storage module for {}", _volumeId);
    checkClosed();
    _syncTimer.cancel();
    _syncTimer.purge();
    _closed.set(true);
    try {
      List<Future<Void>> syncs = sync(false);
      waitForSyncs(syncs);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    LOGGER.info("finished close of storage module for {}", _volumeId);
  }

  private void waitForSyncs(List<Future<Void>> syncs) {
    for (Future<Void> sync : syncs) {
      try {
        sync.get();
      } catch (ExecutionException e) {
        LOGGER.error("Unknown error while syncing", e.getCause());
      } catch (Exception e) {
        LOGGER.error("Unknown error while syncing", e);
      }
    }
  }

  private List<Future<Void>> sync(boolean onlyIfIdleWrites) throws InterruptedException {
    List<Block> blocks = getBlocks();
    List<Callable<Void>> callables = createSyncs(blocks, onlyIfIdleWrites);
    return _syncExecutor.invokeAll(callables);
  }

  private List<Callable<Void>> createSyncs(List<Block> blocks, boolean onlyIfIdleWrites) {
    List<Callable<Void>> callables = new ArrayList<>();
    for (Block block : blocks) {
      if (onlyIfIdleWrites) {
        if (block.idleWrites()) {
          callables.add(() -> {
            sync(block);
            return null;
          });
        }
      } else {
        callables.add(() -> {
          sync(block);
          return null;
        });
      }
    }
    return callables;
  }

  private List<Block> getBlocks() {
    List<Block> blocks = new ArrayList<>();
    ConcurrentMap<BlockKey, Block> map = _cache.asMap();
    for (Entry<BlockKey, Block> entry : map.entrySet()) {
      if (isThisVolume(entry)) {
        blocks.add(entry.getValue());
      }
    }
    return blocks;
  }

  private boolean isThisVolume(Entry<BlockKey, Block> entry) {
    return entry.getKey()
                .getVolumeId() == _volumeId;
  }

  private void sync(Block block) {
    if (block == null) {
      return;
    }
    Utils.runUntilSuccess(LOGGER, () -> {
      LOGGER.info("volume sync volumeId {} blockId {}", _volumeId, block.getBlockId());
      try {
        block.execIO(_externalBlockStoreFactory.getBlockWriter());
      } catch (AlreadyClosedException e) {
        LOGGER.info("volume {} block {} already closed", _volumeId, block.getBlockId());
      }
      return null;
    });
  }

  @Override
  public final int checkBounds(final long logicalBlockAddress, final int transferLengthInBlocks) {
    if (logicalBlockAddress < 0 || logicalBlockAddress > getBlockCount()) {
      return 1;
    } else if (transferLengthInBlocks < 0 || logicalBlockAddress + transferLengthInBlocks > getBlockCount()) {
      return 2;
    } else {
      return 0;
    }
  }

  @Override
  public long getSizeInBlocks() {
    return getBlockCount() - 1;
  }

  private long getBlockCount() {
    return _lengthInBytes / VIRTUAL_BLOCK_SIZE;
  }

  private void checkClosed() throws IOException {
    if (_closed.get()) {
      throw new IOException("already closed");
    }
  }

  private int getBlockOffset(long position) {
    return (int) (position % _blockSize);
  }

  private long getBlockId(long position) {
    return position / _blockSize;
  }

  private TimerTask getTask() {
    return new TimerTask() {
      @Override
      public void run() {
        LOGGER.info("incremental sync for volumeId {}", _volumeId);
        try {
          sync(true);
        } catch (Exception e) {
          LOGGER.error("Unknown error", e);
        }
      }
    };
  }
}
