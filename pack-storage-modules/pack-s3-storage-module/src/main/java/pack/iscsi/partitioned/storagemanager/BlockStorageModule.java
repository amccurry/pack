package pack.iscsi.partitioned.storagemanager;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.LoadingCache;

import pack.iscsi.external.ExternalBlockIOFactory;
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

  public BlockStorageModule(LoadingCache<BlockKey, Block> cache, long volumeId, int blockSize, long lengthInBytes,
      ExternalBlockIOFactory externalBlockStoreFactory) {
    _externalBlockStoreFactory = externalBlockStoreFactory;
    _cache = cache;
    _volumeId = volumeId;
    _blockSize = blockSize;
    _lengthInBytes = lengthInBytes;
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
    _closed.set(true);
    sync();
  }

  private void sync() {
    ConcurrentMap<BlockKey, Block> map = _cache.asMap();
    for (Entry<BlockKey, Block> entry : map.entrySet()) {
      if (entry.getKey()
               .getVolumeId() == _volumeId) {
        Block block = entry.getValue();
        Utils.runUntilSuccess(LOGGER, () -> {
          block.execIO(_externalBlockStoreFactory.getBlockWriter());
          return null;
        });
      }
    }
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
}
