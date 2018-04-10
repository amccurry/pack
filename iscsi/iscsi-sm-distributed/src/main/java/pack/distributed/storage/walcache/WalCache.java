package pack.distributed.storage.walcache;

import java.io.IOException;

import pack.distributed.storage.hdfs.BlockFile.Writer;
import pack.distributed.storage.read.BlockReader;

public interface WalCache extends Comparable<WalCache>, BlockReader {

  long getMaxLayer();

  long getCreationTime();

  void copy(Writer writer) throws IOException;

  void write(long layer, int blockId, byte[] block) throws IOException;

  long getId();

  @Override
  default public int compareTo(WalCache o) {
    return Long.compare(o.getId(), getId());
  }

  int getSize();

  boolean isClosed();

  void incRef();

  void decRef();
  
  int refCount();

}