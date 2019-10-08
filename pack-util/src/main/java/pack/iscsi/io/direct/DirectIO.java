package pack.iscsi.io.direct;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Platform;

import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import pack.iscsi.io.IOUtils;
import pack.iscsi.io.util.NativeFileUtil;
import pack.iscsi.io.util.NativeFileUtil.FallocateMode;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.RandomAccessIOReader;

public class DirectIO implements RandomAccessIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(DirectIO.class);

  private final File _file;
  private final DirectIoLib _lib;
  private final int _fd;
  private final AtomicLong _length = new AtomicLong();
  private final BlockingQueue<AlignedDirectByteBuffer> _buffers = new ArrayBlockingQueue<>(10);
  private final int _bufferSize;

  public DirectIO(File file) throws IOException {
    _file = file;
    _lib = DirectIoLib.getLibForPath(_file.getAbsolutePath());
    _fd = _lib.oDirectOpen(_file.getAbsolutePath(), false);
    _length.set(_file.length());
    _bufferSize = _lib.blockSize() * 1024;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, () -> _lib.close(_fd));
  }

  @Override
  public void read(long position, byte[] buffer, int offset, int length) throws IOException {
    long blockStart = _lib.blockStart(position);
    long blockEnd = _lib.blockEnd(position + length);
    int blength = (int) (blockEnd - blockStart);
    AlignedDirectByteBuffer buf = getAlignedDirectByteBuffer(blength);
    try {
      int remaining = (int) Math.min(buf.limit(), _length.get() - blockStart);
      readInternal(blockStart, buf, remaining);
      buf.flip();
      buf.position((int) (position - blockStart));
      buf.get(buffer, offset, length);
    } finally {
      release(buf);
    }
  }

  private void readInternal(long offset, AlignedDirectByteBuffer buf, int remaining) throws IOException {
    while (remaining > 0) {
      int pread = _lib.pread(_fd, buf, offset);
      buf.position(buf.position() + pread);
      offset += pread;
      remaining -= pread;
    }
  }

  @Override
  public void write(long position, byte[] buffer, int offset, int length) throws IOException {
    long blockStart = _lib.blockStart(position);
    long blockEnd = _lib.blockEnd(position + length);
    int blength = (int) (blockEnd - blockStart);
    long potentialNewLength = position + length;
    if (potentialNewLength > _length.get()) {
      setLength(potentialNewLength);
    }
    AlignedDirectByteBuffer buf = getAlignedDirectByteBuffer(blength);
    try {
      if (!isBlockAligned(position, length, blockStart, blockEnd)) {
        int remaining = (int) Math.min(buf.limit(), _length.get() - blockStart);
        readInternal(blockStart, buf, remaining);
        buf.flip();
      }
      buf.position((int) (position - blockStart));
      buf.put(buffer, offset, length);
      buf.position(0);
      writeInternal(blockStart, buf);
    } finally {
      release(buf);
    }
  }

  private void writeInternal(long offset, AlignedDirectByteBuffer buf) throws IOException {
    int remaining = buf.limit();
    while (remaining > 0) {
      int pwrite = _lib.pwrite(_fd, buf, offset);
      buf.position(Math.min(buf.position() + pwrite, remaining));
      offset += pwrite;
      remaining -= pwrite;
    }
  }

  private boolean isBlockAligned(long position, int length, long blockStart, long blockEnd) {
    return blockStart == position && position + length <= blockEnd;
  }

  private void release(AlignedDirectByteBuffer buf) {
    if (!_buffers.offer(buf)) {
      buf.close();
    }
  }

  private AlignedDirectByteBuffer getAlignedDirectByteBuffer(int length) {
    AlignedDirectByteBuffer buffer = _buffers.poll();
    if (buffer == null) {
      buffer = AlignedDirectByteBuffer.allocate(_lib, _bufferSize);
    }
    buffer.clear();
    buffer.limit(length);
    return buffer;
  }

  @Override
  public void setLength(long length) throws IOException {
    DirectIoLib.ftruncate(_fd, length);
    _length.set(length);
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public long length() throws IOException {
    return _length.get();
  }

  @Override
  public RandomAccessIOReader cloneReadOnly() throws IOException {
    return new RandomAccessIOReader() {

      @Override
      public void close() throws IOException {

      }

      @Override
      public void read(long position, byte[] buffer, int offset, int length) throws IOException {
        DirectIO.this.read(position, buffer, offset, length);
      }

      @Override
      public long length() throws IOException {
        return DirectIO.this.length();
      }
    };
  }

  @Override
  public void punchHole(long position, long length) throws IOException {
    if (Platform.isLinux()) {
      NativeFileUtil.fallocate(_fd, position, length, FallocateMode.FALLOC_FL_PUNCH_HOLE,
          FallocateMode.FALLOC_FL_KEEP_SIZE);
    }
  }

}