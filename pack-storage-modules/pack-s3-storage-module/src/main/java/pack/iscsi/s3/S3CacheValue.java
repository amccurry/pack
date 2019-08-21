package pack.iscsi.s3;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import pack.util.IOUtils;

public class S3CacheValue implements Closeable {

  private static final String RW = "rw";

  public static S3CacheValue createEmptyBlock(File blockFile, int blockSize) throws IOException {
    return new S3CacheValue(blockFile, blockSize, false);
  }

  public static S3CacheValue createExisting(File blockFile, int blockSize) throws IOException {
    return new S3CacheValue(blockFile, blockSize, true);
  }

  private final File _blockFile;
  private final int _blockSize;
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private final AtomicBoolean _dirty = new AtomicBoolean();
  private final AtomicBoolean _closed = new AtomicBoolean();
  private final RandomAccessFile _raf;
  private final FileChannel _channel;

  private S3CacheValue(File blockFile, int blockSize, boolean existing) throws IOException {
    _blockFile = blockFile;
    _blockSize = blockSize;
    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeLock = reentrantReadWriteLock.writeLock();
    _readLock = reentrantReadWriteLock.readLock();
    if (existing) {
      if (_blockFile.length() != _blockSize + 1) {
        throw new IOException("File " + _blockFile + " size does not match " + blockSize + " + 1");
      }
    }
    _raf = new RandomAccessFile(blockFile, RW);
    if (!existing) {
      _raf.setLength(_blockSize + 1);
    }
    _channel = _raf.getChannel();
  }

  public void readFully(long position, byte[] bytes, int offset, int len) throws IOException {
    _readLock.lock();
    checkIfClosed();
    try {
      ByteBuffer dst = ByteBuffer.wrap(bytes, offset, len);
      while (dst.remaining() > 0) {
        position += _channel.read(dst, position);
      }
    } finally {
      _readLock.unlock();
    }
  }

  public void writeFully(long position, byte[] bytes, int offset, int len) throws IOException {
    _writeLock.lock();
    checkIfClosed();
    try {
      if (!isDirty()) {
        markDirty();
      }
      ByteBuffer src = ByteBuffer.wrap(bytes, offset, len);
      while (src.remaining() > 0) {
        position += _channel.write(src, position);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  public boolean isDirty() {
    return _dirty.get();
  }

  public File getBlockFile() {
    return _blockFile;
  }

  public int getBlockSize() {
    return _blockSize;
  }

  @Override
  public void close() throws IOException {
    _writeLock.lock();
    try {
      if (!_closed.get()) {
        _closed.set(true);
        IOUtils.closeQuietly(_channel, _raf);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private void checkIfClosed() throws IOException {
    if (_closed.get()) {
      throw new IOException("File " + _blockFile + " already closed.");
    }
  }

  private void markDirty() throws IOException {
    _channel.write(S3BlockState.DIRTY.toByteBuffer(), _blockSize);
  }
}
