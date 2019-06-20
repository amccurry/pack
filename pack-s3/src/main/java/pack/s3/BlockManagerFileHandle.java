package pack.s3;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import pack.block.BlockManager;
import pack.block.BlockManagerConfig;

public class BlockManagerFileHandle implements Closeable, FileHandle {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockManagerFileHandle.class);

  private final BlockingQueue<byte[]> _buffer;
  private final BlockManager _block;
  private final BlockManagerConfig _config;

  public BlockManagerFileHandle(BlockingQueue<byte[]> buffer, BlockManagerConfig config) throws Exception {
    _buffer = buffer;
    _config = config;
    _block = new BlockManager(config);
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("Close {}", _config);
    IOUtils.closeQuietly(_block);
  }

  public int read(Pointer buf, int size, long offset) throws Exception {
    byte[] buffer = _buffer.take();
    try {
      int len = Math.min(size, buffer.length);
      _block.readFully(offset, buffer, 0, len);
      buf.put(0, buffer, 0, len);
      return len;
    } finally {
      _buffer.put(buffer);
    }
  }

  public int write(Pointer buf, int size, long offset) throws Exception {
    byte[] buffer = _buffer.take();
    try {
      int len = Math.min(size, buffer.length);
      buf.get(0, buffer, 0, len);
      _block.writeFully(offset, buffer, 0, len);
      return len;
    } finally {
      _buffer.put(buffer);
    }
  }
}
