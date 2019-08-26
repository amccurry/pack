package pack.iscsi.partitioned.block;

public interface BlockIOResult {

  BlockState getOnDiskBlockState();

  long getOnDiskGeneration();

  long getLastStoredGeneration();

  public static BlockIOResult newBlockIOResult(long onDiskGeneration, BlockState onDiskState,
      long lastStoredGeneration) {
    return new BlockIOResult() {

      @Override
      public BlockState getOnDiskBlockState() {
        return onDiskState;
      }

      @Override
      public long getOnDiskGeneration() {
        return onDiskGeneration;
      }

      @Override
      public long getLastStoredGeneration() {
        return lastStoredGeneration;
      }
    };
  }

}
