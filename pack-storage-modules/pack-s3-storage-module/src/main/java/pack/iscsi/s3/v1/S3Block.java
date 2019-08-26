package pack.iscsi.s3.v1;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Block implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3Block.class);

  private static final int BLOCK_OVERHEAD = 1;

  private final int _blockSize;
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private final AtomicBoolean _closed = new AtomicBoolean();
  private final FileChannel _channel;
  private final long _blockId;
  private final AtomicReference<S3BlockState> _state = new AtomicReference<>();
  private final AtomicLong _generation = new AtomicLong();

  public S3Block(FileChannel channel, long blockId, int blockSize) throws IOException {
    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeLock = reentrantReadWriteLock.writeLock();
    _readLock = reentrantReadWriteLock.readLock();
    _channel = channel;
    _blockId = blockId;
    _blockSize = blockSize;
  }
  
  public void load() {
    
  }

  /**
   * Position is relative to the block.
   */
  public void readFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException {
    _readLock.lock();
    checkIfClosed();
    checkIfState();
    try {
      long pos = getAbsolutePosition(blockPosition);
      ByteBuffer dst = ByteBuffer.wrap(bytes, offset, len);
      while (dst.remaining() > 0) {
        pos += _channel.read(dst, pos);
      }
    } finally {
      _readLock.unlock();
    }
  }

  /**
   * Position is relative to the block.
   */
  public void writeFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException {
    _writeLock.lock();
    checkIfClosed();
    checkIfState();
    try {
      if (!isDirty()) {
        writeMetadata(S3BlockState.DIRTY, _generation.incrementAndGet());
      }
      long pos = getAbsolutePosition(blockPosition);
      ByteBuffer src = ByteBuffer.wrap(bytes, offset, len);
      while (src.remaining() > 0) {
        pos += _channel.write(src, pos);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private void checkIfState() throws IOException {
    S3BlockState state = _state.get();
    if (state == null || state == S3BlockState.MISSING) {
      throw new IOException("Block " + _blockId + " state " + state + " is invalid for reading/writing.");
    }
  }

  public void exec(S3BlockIOExecutor blockIO) throws IOException {
    _writeLock.lock();
    try {
      checkIfClosed();
      writeMetadata(blockIO.exec(_channel, getAbsoluteStartPositionOfBlock(), _blockSize) );
    } finally {
      _writeLock.unlock();
    }
  }

  public S3BlockState getState() {
    return _state.get();
  }

  public int getBlockSize() {
    return _blockSize;
  }

  public long getBlockId() {
    return _blockId;
  }

  @Override
  public void close() throws IOException {
    _writeLock.lock();
    try {
      if (!_closed.get()) {
        _closed.set(true);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private void checkIfClosed() throws IOException {
    if (_closed.get()) {
      throw new IOException("Block " + _blockId + " already closed.");
    }
  }

  private void readMetaData() throws IOException {
    ByteBuffer dst = ByteBuffer.allocate(BLOCK_OVERHEAD);
    _channel.read(dst, getMetadataPosition());
    dst.flip();
    _state.set(S3BlockState.lookup(dst.get()));
  }

  private void writeMetadata(S3BlockMetadata metadata) throws IOException {
    _state.set(metadata.getState());
    _channel.write(metadata.toByteBuffer(), getMetadataPosition());
    LOGGER.info("write meta data blockId {} state {}", _blockId, _state);
  }

  public boolean isDirty() {
    return getState() == S3BlockState.DIRTY;
  }

  public void clearLocalData() throws IOException {
    LOGGER.info("CLEAR LOCAL DATA TODO");
    byte[] bs = new byte[1024];
    int length = _blockSize;
    long position = getAbsoluteStartPositionOfBlock();
    while (length > 0) {
      int len = Math.min(length, bs.length);
      int write = _channel.write(ByteBuffer.wrap(bs, 0, len), position);
      position += write;
      length -= write;
    }
  }

  public static long newFileLength(long blocks, int blockSize) {
    return (blockSize + BLOCK_OVERHEAD) * blocks;
  }

  private long getAbsolutePosition(long blockPosition) {
    return getAbsoluteStartPositionOfBlock() + blockPosition;
  }

  private long getAbsoluteStartPositionOfBlock() {
    return (_blockSize + BLOCK_OVERHEAD) * _blockId;
  }

  private long getMetadataPosition() {
    return getAbsoluteStartPositionOfBlock() + _blockSize;
  }
}
