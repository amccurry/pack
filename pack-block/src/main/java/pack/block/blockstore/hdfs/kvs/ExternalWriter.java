package pack.block.blockstore.hdfs.kvs;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;

public interface ExternalWriter extends Closeable {

  void write(long key, BytesWritable writable) throws IOException;

  default void commit() throws IOException {

  }

  default void close() throws IOException {

  }

}
