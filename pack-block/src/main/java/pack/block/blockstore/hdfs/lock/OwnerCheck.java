package pack.block.blockstore.hdfs.lock;

import java.io.IOException;

public interface OwnerCheck {
  public boolean isLockOwner() throws IOException;
}
