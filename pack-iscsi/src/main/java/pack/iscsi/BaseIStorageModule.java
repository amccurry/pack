package pack.iscsi;

import org.jscsi.target.storage.IStorageModule;

public abstract class BaseIStorageModule implements IStorageModule {

  private final long _sizeInBlocks;

  public BaseIStorageModule(long sizeInBytes) {
    _sizeInBlocks = sizeInBytes / VIRTUAL_BLOCK_SIZE;
  }

  @Override
  public final int checkBounds(final long logicalBlockAddress, final int transferLengthInBlocks) {
    if (logicalBlockAddress < 0 || logicalBlockAddress >= _sizeInBlocks) {
      return 1;
    }
    if (transferLengthInBlocks < 0 || logicalBlockAddress + transferLengthInBlocks > _sizeInBlocks) {
      return 2;
    }
    return 0;
  }

  @Override
  public long getSizeInBlocks() {
    return _sizeInBlocks;
  }

}
