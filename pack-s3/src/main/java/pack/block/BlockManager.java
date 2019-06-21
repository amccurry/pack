package pack.block;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;

public class BlockManager implements Block {

  public static final long MAX_VOLUME_SIZE = 10_000_000_000_000L;

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockManager.class);

  private final long _blockSize;
  private final LoadingCache<Long, Block> _cache;
  private final BlockFactory _blockFactory;
  private final CrcBlockManager _crcBlockManager;
  private final ExecutorService _service;
  private final WriteLock _writeSyncLock;
  private final ReadLock _readSyncLock;
  private final String _volume;
  private final AtomicLong _lastWriteTime = new AtomicLong();
  private final Timer _timer;
  private final long _idleTime = TimeUnit.SECONDS.toNanos(15);

  public BlockManager(BlockManagerConfig config) throws Exception {
    _blockFactory = config.getBlockFactory();
    _blockSize = config.getBlockSize();
    _service = Executors.newFixedThreadPool(10);
    _volume = config.getVolume();

    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeSyncLock = reentrantReadWriteLock.writeLock();
    _readSyncLock = reentrantReadWriteLock.readLock();

    _timer = new Timer(_volume, true);
    _timer.scheduleAtFixedRate(getTask(), TimeUnit.NANOSECONDS.toMillis(_idleTime),
        TimeUnit.NANOSECONDS.toMillis(_idleTime));

    // if (_useBulkCrc) {
    // BlockConfig crcConfig = BlockConfig.builder()
    // .blockId(Long.MAX_VALUE)
    // .blockSize(getCrcBlockSize(config.getBlockSize()))
    // .crcBlockManager(config.getCrcBlockManager())
    // .volume(config.getVolume())
    // .build();
    //
    // Block crcBlock = _blockFactory.createBlock(crcConfig);
    //
    //
    //
    // _crcBlockManager = CrcBlockManager.create(crcBlock);
    // } else {
    _crcBlockManager = config.getCrcBlockManager();
    // }

    BlockConfig baseConfig = BlockConfig.builder()
                                        .blockSize(_blockSize)
                                        .crcBlockManager(_crcBlockManager)
                                        .volume(config.getVolume())
                                        .build();

    _cache = CacheBuilder.newBuilder()
                         .removalListener(getRemovalListener())
                         .maximumSize(config.getCacheSize() / _blockSize)
                         .build(getCacheLoader(baseConfig));
  }

  private TimerTask getTask() {
    return new TimerTask() {
      @Override
      public void run() {
        doSync(true);
      }
    };
  }

  @Override
  public long getIdleTime() {
    return System.nanoTime() - _lastWriteTime.get();
  }

  @Override
  public int read(long position, byte[] buf, int off, int len) throws Exception {
    _readSyncLock.lock();
    try {
      long blockId = getBlockId(position);
      long relativeOffset = getRelativeOffset(position);

      LOGGER.debug("read position {} blockId {} relativeOffset {} buffer len {} off {} len {}", position, blockId,
          relativeOffset, buf.length, off, len);

      Block block = _cache.get(blockId);
      return block.read(relativeOffset, buf, off, len);
    } finally {
      _readSyncLock.unlock();
    }
  }

  @Override
  public int write(long position, byte[] buf, int off, int len) throws Exception {
    _readSyncLock.lock();
    try {
      long blockId = getBlockId(position);
      long relativeOffset = getRelativeOffset(position);

      LOGGER.debug("write position {} blockId {} relativeOffset {} buffer len {} off {} len {}", position, blockId,
          relativeOffset, buf.length, off, len);

      Block block = _cache.get(blockId);
      return block.write(relativeOffset, buf, off, len);
    } finally {
      _readSyncLock.unlock();
    }
  }

  @Override
  public void sync() {
    doSync(false);
  }

  private void doSync(boolean idleOnly) {
    LOGGER.info("sync for volume {} started", _volume);
    Collection<Block> blocks = getBlocks();
    List<Future<Void>> futures = new ArrayList<>();
    for (Block block : blocks) {
      futures.add(_service.submit(() -> {
        if (!idleOnly || isIdle(block)) {
          block.sync();
        }
        return null;
      }));
    }
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e.getCause());
      }
    }
    _crcBlockManager.sync();
    LOGGER.info("sync for volume {} complete", _volume);
  }

  private boolean isIdle(Block block) {
    return block.getIdleTime() > _idleTime;
  }

  private Collection<Block> getBlocks() {
    Collection<Block> collection;
    _writeSyncLock.lock();
    try {
      collection = _cache.asMap()
                         .values();
    } finally {
      _writeSyncLock.unlock();
    }
    return collection;
  }

  @Override
  public void close() {
    _timer.cancel();
    _timer.purge();
    sync();
    _cache.invalidateAll();
    _crcBlockManager.close();
  }

  private long getRelativeOffset(long offset) {
    return offset % _blockSize;
  }

  private long getBlockId(long offset) {
    return offset / _blockSize;
  }

  private RemovalListener<Long, Block> getRemovalListener() {
    return notification -> {
      Long blockId = notification.getKey();
      LOGGER.debug("volume {} remove block {} from cache", _volume, blockId);
      Block block = notification.getValue();
      block.sync();
      block.close();
    };
  }

  private CacheLoader<Long, Block> getCacheLoader(BlockConfig baseConfig) {
    return new CacheLoader<Long, Block>() {
      @Override
      public Block load(Long blockId) throws Exception {
        LOGGER.debug("volume {} load block {} into cache", _volume, blockId);
        return _blockFactory.createBlock(baseConfig.toBuilder()
                                                   .blockId(blockId)
                                                   .build());
      }
    };
  }

}
