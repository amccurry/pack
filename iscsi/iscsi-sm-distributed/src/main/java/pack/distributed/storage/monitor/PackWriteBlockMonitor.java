package pack.distributed.storage.monitor;

public interface PackWriteBlockMonitor {

  PackWriteBlockMonitor NO_OP = new PackWriteBlockMonitor() {
  };

  default void resetDirtyBlock(int blockId) {

  }

  default void addDirtyBlock(int blockId) {

  }

  default boolean isBlockDirty(int blockId) {
    return true;
  }

}
