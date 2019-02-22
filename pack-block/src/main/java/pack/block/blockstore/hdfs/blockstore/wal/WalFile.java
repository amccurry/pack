package pack.block.blockstore.hdfs.blockstore.wal;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;

public class WalFile {

  public static abstract class Reader implements Closeable {
    public abstract boolean next(WalKeyWritable key, BytesWritable value) throws IOException;
  }

  public static abstract class Writer implements Closeable {
    /**
     * Append the data to the WAL.
     * 
     * @param key
     * @param value
     * @throws IOException
     */
    public abstract void append(WalKeyWritable key, BytesWritable value) throws IOException;

    /**
     * Get the number of bytes written to the writer.
     * 
     * @return
     * @throws IOException
     */
    public abstract long getSize() throws IOException;

    /**
     * Forces the data to be flushed.
     * 
     * @throws IOException
     */
    public void flush() throws IOException {
      flush(false);
    }

    /**
     * Forces the data to be flushed.
     * 
     * @throws IOException
     */
    public abstract void flush(boolean force) throws IOException;

    /**
     * Is this WAL file in an error state.
     * 
     * @return
     */
    public abstract boolean isErrorState();
  }

}
