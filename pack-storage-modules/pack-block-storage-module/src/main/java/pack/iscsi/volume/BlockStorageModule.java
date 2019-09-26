package pack.iscsi.volume;

import java.io.Closeable;
import java.io.File;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;

import io.opencensus.common.Scope;
import pack.iscsi.block.AlreadyClosedException;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockKey;
import pack.iscsi.spi.metric.Meter;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.iscsi.spi.metric.TimerContext;
import pack.iscsi.spi.wal.BlockJournalResult;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.util.Utils;
import pack.iscsi.volume.cache.BlockCacheLoader;
import pack.iscsi.volume.cache.BlockCacheLoaderConfig;
import pack.iscsi.volume.cache.BlockRemovalListener;
import pack.iscsi.volume.cache.BlockRemovalListenerConfig;
import pack.util.ExecutorUtil;
import pack.util.TracerUtil;

public class BlockStorageModule implements StorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModule.class);

  private static final String WRITE = "write-bytes";
  private static final String READ = "read-bytes";
  private static final String READ_IOPS = "read-iops";
  private static final String WRITE_IOPS = "write-iops";

  private final long _volumeId;
  private final int _blockSize;
  private final AtomicLong _lengthInBytes = new AtomicLong();
  private final LoadingCache<BlockKey, Block> _cache;
  private final AtomicBoolean _closed = new AtomicBoolean();
  private final BlockIOFactory _externalBlockStoreFactory;
  private final Timer _syncTimer;
  private final ExecutorService _syncExecutor;
  private final MetricsFactory _metricsFactory;
  private final Meter _readMeter;
  private final Meter _writeMeter;
  private final File _blockDataDir;
  private final BlockWriteAheadLog _writeAheadLog;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;
  private final BlockGenerationStore _blockGenerationStore;
  private final AtomicInteger _refCounter = new AtomicInteger();
  private final Meter _readIOMeter;
  private final Meter _writeIOMeter;
  private final TimerContext _readTimer;
  private final TimerContext _writeTimer;

  public BlockStorageModule(BlockStorageModuleConfig config) throws IOException {
    _blockGenerationStore = config.getBlockGenerationStore();
    _syncTimeAfterIdle = config.getSyncTimeAfterIdle();
    _syncTimeAfterIdleTimeUnit = config.getSyncTimeAfterIdleTimeUnit();
    _blockDataDir = config.getBlockDataDir();
    _writeAheadLog = config.getWriteAheadLog();
    _metricsFactory = config.getMetricsFactory();
    String volumeName = config.getVolumeName();
    _readMeter = _metricsFactory.meter(BlockStorageModule.class, volumeName, READ);
    _readIOMeter = _metricsFactory.meter(BlockStorageModule.class, volumeName, READ_IOPS);
    _writeMeter = _metricsFactory.meter(BlockStorageModule.class, volumeName, WRITE);
    _writeIOMeter = _metricsFactory.meter(BlockStorageModule.class, volumeName, WRITE_IOPS);
    _readTimer = _metricsFactory.timer(BlockStorageModule.class, volumeName, "read-timer");
    _writeTimer = _metricsFactory.timer(BlockStorageModule.class, volumeName, "write-timer");

    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();

    _volumeId = config.getVolumeId();
    _blockSize = config.getBlockSize();
    _lengthInBytes.set(config.getLengthInBytes());
    _syncExecutor = config.getSyncExecutor();
    _syncTimer = new Timer("sync-" + _volumeId);

    long period = config.getSyncTimeAfterIdleTimeUnit()
                        .toMillis(config.getSyncTimeAfterIdle());
    _syncTimer.schedule(getTask(), period, period);

    BlockRemovalListener removalListener = getRemovalListener();

    BlockCacheLoader loader = getCacheLoader(removalListener);

    Weigher<BlockKey, Block> weigher = (key, value) -> value.getSize();

    _cache = Caffeine.newBuilder()
                     .executor(ExecutorUtil.getCallerRunExecutor())
                     .removalListener(removalListener)
                     .weigher(weigher)
                     .maximumWeight(config.getMaxCacheSizeInBytes())
                     .build(loader);

    preloadBlockInfo();
  }

  private void preloadBlockInfo() throws IOException {
    long blockCount = _lengthInBytes.get() / (long) _blockSize;
    _blockGenerationStore.preloadGenerationInfo(_volumeId, blockCount);
  }

  public void setLengthInBytes(long lengthInBytes) throws IOException {
    long current = _lengthInBytes.get();
    if (lengthInBytes < current) {
      throw new IOException("new length of " + lengthInBytes + " is less than current length " + current);
    }
    LOGGER.info("Updating the current length of volume id {} to {}", _volumeId, lengthInBytes);
    _lengthInBytes.set(lengthInBytes);
    preloadBlockInfo();
  }

  public void incrementRef() {
    _refCounter.incrementAndGet();
  }

  public void decrementRef() {
    _refCounter.decrementAndGet();
  }

  public int getRefCount() {
    return _refCounter.get();
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
      LOGGER.info("waiting for syncs to complete");
      waitForSyncs(syncs);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    LOGGER.info("finished close of storage module for {}", _volumeId);
  }

  @Override
  public void read(byte[] bytes, long position) throws IOException {
    checkClosed();
    LOGGER.info("read volumeId {} length {} position {}", _volumeId, bytes.length, position);
    int length = bytes.length;
    _readMeter.mark(length);
    _readIOMeter.mark();
    int offset = 0;
    try (Closeable time = _readTimer.time()) {
      try (Scope readScope = TracerUtil.trace(BlockStorageModule.class, READ)) {
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

          try (Scope blockWriterScope = TracerUtil.trace(BlockStorageModule.class, "block read")) {
            block.readFully(blockOffset, bytes, offset, len);
          }
          length -= len;
          position += len;
          offset += len;
        }
      }
    }
  }

  private List<BlockJournalResult> _results = new ArrayList<>();
  private final AtomicLong _writesCount = new AtomicLong();

  @Override
  public void write(byte[] bytes, long position) throws IOException {
    checkClosed();
    LOGGER.info("write volumeId {} length {} position {}", _volumeId, bytes.length, position);
    _writesCount.addAndGet(bytes.length);
    int length = bytes.length;
    _writeMeter.mark(length);
    _writeIOMeter.mark();
    int offset = 0;
    try (Closeable time = _writeTimer.time()) {
      try (Scope writeScope = TracerUtil.trace(BlockStorageModule.class, WRITE)) {
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
          try (Scope blockWriterScope = TracerUtil.trace(BlockStorageModule.class, "block write")) {
            trackResult(block.writeFully(blockOffset, bytes, offset, len));
          }
          length -= len;
          position += len;
          offset += len;
        }
      }
    }
  }

  @Override
  public void flushWrites() throws IOException {
    int size = _results.size();
    long writeCount = _writesCount.getAndSet(0);
    long start = System.nanoTime();
    try (Scope writeScope = TracerUtil.trace(BlockStorageModule.class, "flushWrites")) {
      for (BlockJournalResult result : _results) {
        result.get();
      }
      _results.clear();
    }
    long end = System.nanoTime();
    LOGGER.info("flushWrites {} {} in {} ms", size, writeCount, (end - start) / 1_000_000.0);
  }

  private void trackResult(BlockJournalResult result) {
    _results.add(result);
  }

  private Block getBlockId(BlockKey blockKey) {
    try (Scope scope = TracerUtil.trace(BlockStorageModule.class, "get block")) {
      return _cache.get(blockKey);
    }
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
      Callable<Void> callable = () -> {
        LOGGER.info("starting sync for block id {} from volume id", block.getBlockId(), block.getVolumeId());
        sync(block);
        LOGGER.info("finished sync for block id {} from volume id", block.getBlockId(), block.getVolumeId());
        return null;
      };
      if (onlyIfIdleWrites) {
        if (block.idleWrites()) {
          callables.add(callable);
        }
      } else {
        callables.add(callable);
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

  @Override
  public long getSizeInBytes() {
    return _lengthInBytes.get();
  }

  private long getBlockCount() {
    return _lengthInBytes.get() / VIRTUAL_BLOCK_SIZE;
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

  private BlockRemovalListener getRemovalListener() {
    return new BlockRemovalListener(BlockRemovalListenerConfig.builder()
                                                              .externalBlockStoreFactory(_externalBlockStoreFactory)
                                                              .build());
  }

  private BlockCacheLoader getCacheLoader(BlockRemovalListener removalListener) {
    return new BlockCacheLoader(BlockCacheLoaderConfig.builder()
                                                      .blockDataDir(_blockDataDir)
                                                      .blockGenerationStore(_blockGenerationStore)
                                                      .blockSize(_blockSize)
                                                      .externalBlockStoreFactory(_externalBlockStoreFactory)
                                                      .removalListener(removalListener)
                                                      .syncTimeAfterIdle(_syncTimeAfterIdle)
                                                      .syncTimeAfterIdleTimeUnit(_syncTimeAfterIdleTimeUnit)
                                                      .volumeId(_volumeId)
                                                      .writeAheadLog(_writeAheadLog)
                                                      .build());
  }

}
