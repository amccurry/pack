package pack.distributed.storage.wal;

import java.io.Closeable;
import java.io.IOException;

import pack.distributed.storage.read.BlockReader;

public interface WalCacheManager extends Closeable, BlockReader {

  long getMaxLayer();

  void writeWalCacheToHdfs() throws IOException;

  void removeOldWalCache() throws IOException;

  void write(long transId, long offset, int blockId, byte[] bs) throws IOException;

}
