package pack.iscsi.spi;

public abstract class BaseStorageModule implements StorageModule {

  private final long _sizeInBlocks;

  public BaseStorageModule(long sizeInBytes) {
    _sizeInBlocks = sizeInBytes / VIRTUAL_BLOCK_SIZE;
  }

  @Override
  public final int checkBounds(final long logicalBlockAddress, final int transferLengthInBlocks) {
    if (logicalBlockAddress < 0 || logicalBlockAddress > _sizeInBlocks) {
      return 1;
    } else if (transferLengthInBlocks < 0 || logicalBlockAddress + transferLengthInBlocks > _sizeInBlocks) {
      return 2;
    } else {
      return 0;
    }
  }

  @Override
  public long getSizeInBlocks() {
    return _sizeInBlocks - 1;
  }

}
