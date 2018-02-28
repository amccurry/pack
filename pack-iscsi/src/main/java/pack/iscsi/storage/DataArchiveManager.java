package pack.iscsi.storage;

import java.io.Closeable;
import java.io.IOException;

import pack.block.blockstore.hdfs.blockstore.WalFile.Writer;

public interface DataArchiveManager extends Closeable {

  BlockReader getBlockReader() throws IOException;

  Writer createRemoteWalWriter(long offset) throws IOException;

  long getMaxCommitOffset();

}
