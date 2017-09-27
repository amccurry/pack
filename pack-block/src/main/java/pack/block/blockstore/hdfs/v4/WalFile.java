package pack.block.blockstore.hdfs.v4;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;

import pack.block.blockstore.hdfs.file.WalKeyWritable;

public class WalFile {

  public static abstract class Reader implements Closeable {
    public abstract boolean next(WalKeyWritable key, BytesWritable value) throws IOException;
  }

  public static abstract class Writer implements Closeable {
    public abstract void append(WalKeyWritable key, BytesWritable value) throws IOException;

    public abstract long getLength() throws IOException;

    public abstract void flush() throws IOException;
  }

}
