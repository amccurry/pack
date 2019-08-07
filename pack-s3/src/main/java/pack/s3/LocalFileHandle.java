package pack.s3;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.IOUtils;

import jnr.ffi.Pointer;

public class LocalFileHandle implements Closeable, FileHandle {

  private final RandomAccessFile _rand;
  private final FileChannel _channel;
  private final BlockingQueue<byte[]> _buffer;

  public LocalFileHandle(RandomAccessFile randomAccessFile, BlockingQueue<byte[]> buffer) {
    _buffer = buffer;
    _rand = randomAccessFile;
    _channel = _rand.getChannel();
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(_channel);
    IOUtils.closeQuietly(_rand);
  }

  public int read(Pointer buf, int size, long offset) throws Exception {
    byte[] buffer = _buffer.take();
    try {
      ByteBuffer bb = ByteBuffer.wrap(buffer);
      bb.limit(size);
      int read = _channel.read(bb, offset);
      if (read < 0) {
        return 0;
      }
      buf.put(0, buffer, 0, read);
      return read;
    } finally {
      _buffer.put(buffer);
    }
  }

  public int write(Pointer buf, int size, long offset) throws Exception {
    byte[] buffer = _buffer.take();
    try {
      buf.get(0, buffer, 0, size);
      ByteBuffer bb = ByteBuffer.wrap(buffer);
      bb.limit((int) size);
      return _channel.write(bb, offset);
    } finally {
      _buffer.put(buffer);
    }
  }

  @Override
  public void delete(long offset, long length) throws Exception {

  }

  @Override
  public String getVolumeName() {
    return null;
  }

  @Override
  public void incRef() {

  }
}
