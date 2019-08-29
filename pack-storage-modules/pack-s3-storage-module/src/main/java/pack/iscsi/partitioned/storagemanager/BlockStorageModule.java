package pack.iscsi.partitioned.storagemanager;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.LoadingCache;

import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.partitioned.block.AlreadyClosedException;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.util.Utils;
import pack.iscsi.spi.StorageModule;

public class BlockStorageModule implements StorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockStorageModule.class);

  private final long _volumeId;
  private final int _blockSize;
  private final long _lengthInBytes;
  private final LoadingCache<BlockKey, Block> _cache;
  private final AtomicBoolean _closed = new AtomicBoolean();
  private final ExternalBlockIOFactory _externalBlockStoreFactory;
  private final Timer _syncTimer;
  private final ExecutorService _syncExecutor;

  public BlockStorageModule(BlockStorageModuleConfig config) {
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
    int offset = 0;
    while (length > 0) {
      long blockId = getBlockId(position);
      int blockOffset = getBlockOffset(position);
      int remaining = _blockSize - blockOffset;
      BlockKey blockKey = BlockKey.builder()
                                  .volumeId(_volumeId)
                                  .blockId(blockId)
                                  .build();
      Block block = _cache.get(blockKey);
      int len = Math.min(remaining, length);
      block.readFully(blockOffset, bytes, offset, len);
      length -= len;
      position += len;
      offset += len;
    }
  }

  @Override
  public void write(byte[] bytes, long position) throws IOException {
    checkClosed();
    int length = bytes.length;
    int offset = 0;
    while (length > 0) {
      long blockId = getBlockId(position);
      int blockOffset = getBlockOffset(position);
      int remaining = _blockSize - blockOffset;
      BlockKey blockKey = BlockKey.builder()
                                  .volumeId(_volumeId)
                                  .blockId(blockId)
                                  .build();
      Block block = _cache.get(blockKey);
      int len = Math.min(remaining, length);
      block.writeFully(blockOffset, bytes, offset, len);
      length -= len;
      position += len;
      offset += len;
    }
  }

  @Override
  public void flushWrites() throws IOException {

  }

  @Override
  public void close() throws IOException {
    LOGGER.info("close storage module for {}", _volumeId);
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
      callables.add(() -> {
        if (onlyIfIdleWrites) {
          if (block.idleWrites()) {
            sync(block);
          }
        } else {
          sync(block);
        }
        return null;
      });
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
        try {
          sync(true);
        } catch (Exception e) {
          LOGGER.error("Unknown error", e);
        }
      }
    };
  }
}
