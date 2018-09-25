package pack.block.blockstore.hdfs.lock;

import java.io.Closeable;
import java.io.IOException;

public interface PackLock extends Closeable, OwnerCheck {

  boolean isLockOwner() throws IOException;

  boolean tryToLock() throws IOException;

}