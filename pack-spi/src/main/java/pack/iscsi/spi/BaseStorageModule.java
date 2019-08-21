package pack.iscsi.spi;

public abstract class BaseStorageModule implements StorageModule {

  private final long _sizeInBytes;

  public BaseStorageModule(long sizeInBytes) {
    _sizeInBytes = sizeInBytes;
  }

  private long getBlocks(long sizeInBytes) {
    return sizeInBytes / VIRTUAL_BLOCK_SIZE;
  }

  @Override
  public final int checkBounds(final long logicalBlockAddress, final int transferLengthInBlocks) {
    if (logicalBlockAddress < 0 || logicalBlockAddress > getBlocks(_sizeInBytes)) {
      return 1;
    } else if (transferLengthInBlocks < 0 || logicalBlockAddress + transferLengthInBlocks > getBlocks(_sizeInBytes)) {
      return 2;
    } else {
      return 0;
    }
  }

  @Override
  public long getSizeInBlocks() {
    return getBlocks(_sizeInBytes) - 1;
  }

}
