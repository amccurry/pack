package pack.iscsi.storage;

import org.jscsi.target.storage.IStorageModule;

public abstract class BaseStorageModule implements IStorageModule {

  private final long _sizeInBlocks;
  private final int _blockSize;

  public BaseStorageModule(long sizeInBytes, int blockSize, String name) {
    _blockSize = blockSize;
    _sizeInBlocks = sizeInBytes / blockSize;
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

  @Override
  public int getBlockSize() {
    return _blockSize;
  }

  public int getBlockId(long pos) {
    return (int) (pos / _blockSize);
  }

  public int getBlockOffset(long pos) {
    return (int) (pos % _blockSize);
  }

}
