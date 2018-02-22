package pack.iscsi;

import org.jscsi.target.storage.IStorageModule;

public abstract class BaseIStorageModule implements IStorageModule {

  private final long _sizeInBlocks;

  public BaseIStorageModule(long sizeInBytes) {
    _sizeInBlocks = sizeInBytes / VIRTUAL_BLOCK_SIZE;
  }

  @Override
  public final int checkBounds(final long logicalBlockAddress, final int transferLengthInBlocks) {
    if (logicalBlockAddress < 0 || logicalBlockAddress > _sizeInBlocks) {
      // System.out.println(logicalBlockAddress + " " + sizeInBlocks);
      return 1;
    } else if (transferLengthInBlocks < 0 || logicalBlockAddress + transferLengthInBlocks > _sizeInBlocks) {
      // System.out.println(transferLengthInBlocks + " < 0 || " +
      // logicalBlockAddress + " + " + transferLengthInBlocks
      // + " > " + sizeInBlocks);
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
