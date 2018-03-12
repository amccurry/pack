package pack.distributed.storage.monitor;

public interface WriteBlockMonitor {

  WriteBlockMonitor NO_OP = new WriteBlockMonitor() {
  };

  default void resetDirtyBlock(int blockId, long transId) {

  }

  default void addDirtyBlock(int blockId, long transId) {

  }

  default void waitIfNeededForSync(int blockId) {

  }

  default long createTransId() {
    return System.currentTimeMillis();
  }

}
