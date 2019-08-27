package pack.iscsi.partitioned.storagemanager;

import java.io.IOException;

import com.github.benmanes.caffeine.cache.LoadingCache;

import pack.iscsi.partitioned.block.Block;
import pack.iscsi.spi.StorageModule;

public class BlockStorageModule implements StorageModule {

  private final long _volumeId;
  private final int _blockSize;
  private final long _lengthInBytes;
  private final LoadingCache<BlockKey, Block> _cache;

  public BlockStorageModule(LoadingCache<BlockKey, Block> cache, long volumeId, int blockSize, long lengthInBytes) {
    _cache = cache;
    _volumeId = volumeId;
    _blockSize = blockSize;
    _lengthInBytes = lengthInBytes;
  }

  @Override
  public void read(byte[] bytes, long position) throws IOException {
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

  }

  private int getBlockOffset(long position) {
    return (int) (position % _blockSize);
  }

  private long getBlockId(long position) {
    return position / _blockSize;
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

}
