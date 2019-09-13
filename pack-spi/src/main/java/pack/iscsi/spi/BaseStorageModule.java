package pack.iscsi.spi;

public abstract class BaseStorageModule implements StorageModule {

  private final long _sizeInBytes;

  public BaseStorageModule(long sizeInBytes) {
    _sizeInBytes = sizeInBytes;
  }

  private long getBlocks(long sizeInBytes) {
    return sizeInBytes / getBlockSize();
  }

  @Override
  public final int checkBounds(final long logicalBlockAddress, final int transferLengthInBlocks) {
    long blocks = getBlocks(_sizeInBytes);
    if (logicalBlockAddress < 0 || logicalBlockAddress > blocks) {
      return 1;
    } else if (transferLengthInBlocks < 0 || logicalBlockAddress + transferLengthInBlocks > blocks) {
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
