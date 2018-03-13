package pack.distributed.storage.monitor;

import pack.iscsi.storage.utils.PackUtils;

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
    return PackUtils.getRandomLong();
  }

}
