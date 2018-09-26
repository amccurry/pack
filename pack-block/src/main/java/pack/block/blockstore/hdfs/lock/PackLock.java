package pack.block.blockstore.hdfs.lock;

import java.io.Closeable;
import java.io.IOException;

import pack.block.util.PackSizeOf;

public interface PackLock extends Closeable, OwnerCheck, PackSizeOf {

  boolean isLockOwner() throws IOException;

  boolean tryToLock() throws IOException;

}