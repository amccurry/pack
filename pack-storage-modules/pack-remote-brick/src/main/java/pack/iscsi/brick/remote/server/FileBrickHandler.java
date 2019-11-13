package pack.iscsi.brick.remote.server;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class FileBrickHandler implements Closeable {

  private static final long META_LENGTH = 8;
  private static final String RW = "rw";

  private final RandomAccessFile _raf;
  private final FileChannel _channel;
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private final long _metaDataOffset;

  public FileBrickHandler(File file, long blockSize) throws IOException {
    _raf = new RandomAccessFile(file, RW);
    _raf.setLength(blockSize + META_LENGTH);
    _channel = _raf.getChannel();
    _metaDataOffset = blockSize;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    _writeLock = lock.writeLock();
    _readLock = lock.readLock();
  }

  public long read(ByteBuffer dst, long position) throws IOException {
    try {
      _readLock.lock();
      while (dst.hasRemaining()) {
        position += _channel.read(dst, position);
      }
      return getGenerationId();
    } finally {
      _readLock.unlock();
    }
  }

  private long getGenerationId() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    long position = _metaDataOffset;
    while (buffer.hasRemaining()) {
      position += _channel.read(buffer, position);
    }
    buffer.flip();
    return buffer.getLong();
  }

  public long write(ByteBuffer src, long position) throws IOException {
    try {
      _writeLock.lock();
      long currentGenerationId = getGenerationId();
      while (src.hasRemaining()) {
        position += _channel.write(src, position);
      }
      currentGenerationId++;
      setGenerationId(currentGenerationId);
      return currentGenerationId;
    } finally {
      _writeLock.unlock();
    }
  }

  public long write(ByteBuffer src, long position, long generationId) throws IOException {
    try {
      _writeLock.lock();
      while (src.hasRemaining()) {
        position += _channel.write(src, position);
      }
      setGenerationId(generationId);
      return generationId;
    } finally {
      _writeLock.unlock();
    }
  }

  private void setGenerationId(long generationId) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(generationId);
    buffer.flip();
    long position = _metaDataOffset;
    while (buffer.hasRemaining()) {
      position += _channel.write(buffer, position);
    }
  }

  @Override
  public void close() throws IOException {
    _channel.close();
    _raf.close();
  }

}
