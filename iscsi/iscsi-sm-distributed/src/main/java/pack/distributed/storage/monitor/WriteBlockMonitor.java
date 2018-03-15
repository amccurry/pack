package pack.distributed.storage.monitor;

import pack.iscsi.storage.utils.PackUtils;

public interface WriteBlockMonitor {

  WriteBlockMonitor NO_OP = new WriteBlockMonitor() {
  };

  default void resetDirtyBlock(int blockId, long transId) {

  }

  default void addDirtyBlock(int blockId, long transId) {

  }

  default boolean waitIfNeededForSync(int blockId) {
    return false;
  }

  default long createTransId() {
    return PackUtils.getRandomLong();
  }

  default void clearAllLocks() {

  }

}
