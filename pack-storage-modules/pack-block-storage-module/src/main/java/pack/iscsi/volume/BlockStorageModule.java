package pack.iscsi.volume;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

import io.opentracing.Scope;
import pack.iscsi.block.AlreadyClosedException;
import pack.iscsi.concurrent.ConcurrentUtils;
import pack.iscsi.io.FileIO;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.async.AsyncCompletableFuture;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockCacheMetadataStore;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.metric.Meter;
import pack.iscsi.spi.metric.MetricsFactory;
import pack.iscsi.spi.metric.TimerContext;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.util.Utils;
import pack.iscsi.volume.cache.BlockCacheLoader;
import pack.iscsi.volume.cache.BlockCacheLoaderConfig;
import pack.iscsi.volume.cache.BlockRemovalListener;
import pack.iscsi.volume.cache.BlockRemovalListenerConfig;
import pack.util.ExecutorUtil;
import pack.util.tracer.Tag;
import pack.util.tracer.TracerUtil;

public class BlockStorageModule implements StorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModule.class);

  private static final String PRELOAD = "preload-";
  private static final String SYNC = "sync-";
  private static final String RW = "rw";
  private static final String WRITE_TIMER = "write-timer";
  private static final String READ_TIMER = "read-timer";
  private static final String WRITE = "write-bytes";
  private static final String READ = "read-bytes";
  private static final String READ_IOPS = "read-iops";
  private static final String WRITE_IOPS = "write-iops";

  private final long _volumeId;
  private final int _blockSize;
  private final AtomicLong _blockCount = new AtomicLong();
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
  private final RandomAccessIO _randomAccessIO;
  private final BlockStateStore _blockStateStore;
  private final File _file;
  private final List<AsyncCompletableFuture> _results = new ArrayList<>();
  private final AtomicLong _writesCount = new AtomicLong();
  private final ExecutorService _flushExecutor;
  private final ExecutorService _cachePreloadExecutor;
  private final BlockCacheMetadataStore _blockCacheMetadataStore;
  private final boolean _readOnly;
  private final AtomicLong _prefetchRemaining = new AtomicLong();

  public BlockStorageModule(BlockStorageModuleConfig config) throws IOException {
    _readOnly = config.isReadOnly();
    _blockCacheMetadataStore = config.getBlockCacheMetadataStore();
    _flushExecutor = Executors.newSingleThreadExecutor();
    _cachePreloadExecutor = ConcurrentUtils.executor(PRELOAD + config.getVolumeId(),
        config.getCachePreloadExecutorThreadCount());
    _blockStateStore = config.getBlockStateStore();
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
    _readTimer = _metricsFactory.timer(BlockStorageModule.class, volumeName, READ_TIMER);
    _writeTimer = _metricsFactory.timer(BlockStorageModule.class, volumeName, WRITE_TIMER);

    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();

    _volumeId = config.getVolumeId();
    _blockSize = config.getBlockSize();
    _blockCount.set(config.getBlockCount());
    _syncExecutor = ConcurrentUtils.executor(SYNC + config.getVolumeId(), config.getSyncExecutorThreadCount());
    _syncTimer = new Timer(SYNC + _volumeId);

    long period = config.getSyncTimeAfterIdleTimeUnit()
                        .toMillis(config.getSyncTimeAfterIdle());
    if (!_readOnly) {
      _syncTimer.schedule(getTask(), period, period);
    }

    _blockDataDir.mkdirs();

    _file = new File(_blockDataDir, Long.toString(_volumeId));

    _randomAccessIO = FileIO.openRandomAccess(_file, config.getBlockSize(), RW);
    _randomAccessIO.setLength(getLengthInBytes());

    _blockStateStore.createBlockMetadataStore(_volumeId);
    _blockStateStore.setMaxBlockCount(_volumeId, _blockCount.get());

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
    preloadBlockCache();
  }

  private void preloadBlockCache() {
    _cachePreloadExecutor.submit(() -> {
      try {
        preloadBlockCache(_blockCacheMetadataStore.getCachedBlockIds(_volumeId));
      } catch (Exception e) {
        LOGGER.error("Unknown error trying to preload block cache", e);
      }
      return null;
    });
  }

  private void preloadBlockCache(long[] blockIds) {
    LOGGER.info("preloading volume id {}", _volumeId);
    AtomicLong start = new AtomicLong(System.nanoTime());

    _prefetchRemaining.set((long) blockIds.length * _blockSize);
    Object lock = new Object();
    for (int i = 0; i < blockIds.length; i++) {
      int index = i;
      long blockId = blockIds[i];
      BlockKey blockKey = BlockKey.builder()
                                  .blockId(blockId)
                                  .volumeId(_volumeId)
                                  .build();
      _cachePreloadExecutor.submit(() -> {
        LOGGER.debug("preloading volume id {} block id {}", _volumeId, blockId);
        _cache.get(blockKey);
        _prefetchRemaining.addAndGet(-_blockSize);

        synchronized (lock) {
          if (start.get() + TimeUnit.SECONDS.toNanos(5) < System.nanoTime()) {
            LOGGER.info("preload status {} of {} blocks loaded, prefetch remaining total {}", index, blockIds.length,
                _prefetchRemaining.get());
            start.set(System.nanoTime());
          }
        }
        return null;
      });
    }
  }

  public synchronized Map<BlockKey, Long> createSnapshot() throws IOException {
    sync(true, false);
    return _blockGenerationStore.getAllLastStoredGeneration(_volumeId);
  }

  @Override
  public void close() throws IOException {
    if (_closed.get()) {
      return;
    }
    _closed.set(true);
    LOGGER.info("starting close of storage module for {}", _volumeId);
    _syncTimer.cancel();
    _syncTimer.purge();
    if (!_readOnly) {
      try {
        List<Future<Void>> syncs = sync(false);
        LOGGER.info("waiting for syncs to complete");
        waitForSyncs(syncs);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    IOUtils.close(LOGGER, _randomAccessIO);
    IOUtils.close(LOGGER, _flushExecutor, _syncExecutor, _cachePreloadExecutor);
    _file.delete();
    _blockStateStore.destroyBlockMetadataStore(_volumeId);
    LOGGER.info("finished close of storage module for {}", _volumeId);
  }

  private long getLengthInBytes() {
    return _blockCount.get() * _blockSize;
  }

  private void preloadBlockInfo() throws IOException {
    _blockGenerationStore.preloadGenerationInfo(_volumeId, _blockCount.get());
  }

  public void setBlockClount(long newBlockCount) throws IOException {
    checkReadOnly();
    checkClosed();
    long current = _blockCount.get();
    if (newBlockCount < current) {
      throw new IOException("new block count of " + newBlockCount + " is less than current block count " + current);
    }
    LOGGER.info("Updating the current block count of volume id {} to {}", _volumeId, newBlockCount);
    _blockCount.set(newBlockCount);
    _blockStateStore.setMaxBlockCount(_volumeId, newBlockCount);
    preloadBlockInfo();
  }

  public void setLengthInBytes(long lengthInBytes) throws IOException {
    checkReadOnly();
    checkClosed();
    long newBlockCount = Utils.getBlockCount(lengthInBytes, _blockSize);
    setBlockClount(newBlockCount);
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
  public void read(byte[] bytes, long position) throws IOException {
    checkClosed();
    checkLength(bytes, position);
    LOGGER.debug("read volumeId {} length {} position {}", _volumeId, bytes.length, position);
    int length = bytes.length;
    _readMeter.mark(length);
    _readIOMeter.mark();
    int offset = 0;
    try (Closeable time = _readTimer.time()) {
      try (Scope readScope = TracerUtil.trace(BlockStorageModule.class, READ, createTags(bytes, position))) {
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

  private Tag[] createTags(byte[] bytes, long position) {
    return new Tag[] { Tag.create("position", position), Tag.create("length", bytes.length) };
  }

  @Override
  public void write(byte[] bytes, long position) throws IOException {
    checkReadOnly();
    checkClosed();
    checkLength(bytes, position);
    LOGGER.debug("write volumeId {} length {} position {}", _volumeId, bytes.length, position);
    _writesCount.addAndGet(bytes.length);
    int length = bytes.length;
    _writeMeter.mark(length);
    _writeIOMeter.mark();
    int offset = 0;
    try (Closeable time = _writeTimer.time()) {
      try (Scope writeScope = TracerUtil.trace(BlockStorageModule.class, WRITE, createTags(bytes, position))) {
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
          try (Scope blockWriterScope = TracerUtil.trace(BlockStorageModule.class, "block write",
              Tag.create("length", len))) {
            trackResult(block.writeFully(blockOffset, bytes, offset, len));
          }
          length -= len;
          position += len;
          offset += len;
        }
      }
    }
  }

  private void checkLength(byte[] bs, long position) throws EOFException {
    long lengthInBytes = getLengthInBytes();
    if (bs.length + position > lengthInBytes) {
      throw new EOFException(
          "bs len " + bs.length + " at pos " + position + " beyond end of volume with length " + lengthInBytes);
    }
  }

  private void checkReadOnly() throws IOException {
    if (_readOnly) {
      throw new IOException("Read only volume.");
    }
  }

  @Override
  public void flushWrites() throws IOException {
    checkReadOnly();
    checkClosed();
    int size = _results.size();
    long writeCount = _writesCount.getAndSet(0);
    long start = System.nanoTime();
    AsyncCompletableFuture future = AsyncCompletableFuture.exec(BlockStorageModule.class, "flush", _flushExecutor,
        () -> _randomAccessIO.flush());
    try (Scope writeScope = TracerUtil.trace(BlockStorageModule.class, "flushWrites")) {
      for (AsyncCompletableFuture result : _results) {
        result.get();
      }
      _results.clear();
    }
    future.get();
    long end = System.nanoTime();
    LOGGER.debug("flushWrites {} {} in {} ms", size, writeCount, (end - start) / 1_000_000.0);
  }

  private void trackResult(AsyncCompletableFuture completableFuture) {
    _results.add(completableFuture);
  }

  private Block getBlockId(BlockKey blockKey) {
    try (Scope scope1 = TracerUtil.trace(BlockStorageModule.class, "get block")) {
      try (Scope scope2 = TracerUtil.trace(BlockStorageModule.class, "cache cleanup")) {
        _cache.cleanUp();
      }
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

  public void sync(boolean blocking, boolean onlyIfIdleWrites) throws IOException {
    checkClosed();
    if (_readOnly) {
      return;
    }
    try {
      List<Future<Void>> syncs = sync(onlyIfIdleWrites);
      if (blocking) {
        waitForSyncs(syncs);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private List<Future<Void>> sync(boolean onlyIfIdleWrites) throws InterruptedException, IOException {
    List<Block> blocks = getBlocks();
    storeBlockCacheMetadata(blocks);
    List<Callable<Void>> callables = createSyncs(blocks, onlyIfIdleWrites);
    return _syncExecutor.invokeAll(callables);
  }

  private void storeBlockCacheMetadata(List<Block> blocks) throws IOException {
    long[] blockIds = getBlockIds(blocks);
    _blockCacheMetadataStore.setCachedBlockIds(_volumeId, blockIds);
  }

  private long[] getBlockIds(List<Block> blocks) {
    long[] blockIds = new long[blocks.size()];
    int index = 0;
    for (Block block : blocks) {
      blockIds[index++] = block.getBlockId();
    }
    return blockIds;
  }

  private List<Callable<Void>> createSyncs(List<Block> blocks, boolean onlyIfIdleWrites) {
    List<Callable<Void>> callables = new ArrayList<>();
    for (Block block : blocks) {
      Callable<Void> callable = () -> {
        LOGGER.debug("starting sync for block id {} from volume id {}", block.getBlockId(), block.getVolumeId());
        sync(block);
        LOGGER.debug("finished sync for block id {} from volume id {}", block.getBlockId(), block.getVolumeId());
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
    ConcurrentUtils.runUntilSuccess(LOGGER, () -> {
      LOGGER.debug("volume sync volumeId {} blockId {}", _volumeId, block.getBlockId());
      try {
        block.execIO(_externalBlockStoreFactory.getBlockWriter());
      } catch (AlreadyClosedException e) {
        LOGGER.error("volume {} block {} already closed", _volumeId, block.getBlockId());
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
  public int getBlockSize() {
    return 4096;
  }

  @Override
  public long getSizeInBlocks() {
    return getBlockCount() - 1;
  }

  @Override
  public long getSizeInBytes() {
    return getLengthInBytes();
  }

  private long getBlockCount() {
    return getLengthInBytes() / getBlockSize();
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
                                                      .randomAccessIO(_randomAccessIO)
                                                      .blockStateStore(_blockStateStore)
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
