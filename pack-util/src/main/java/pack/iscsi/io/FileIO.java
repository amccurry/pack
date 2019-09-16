package pack.iscsi.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Platform;

import net.smacke.jaydio.DirectRandomAccessFile;
import pack.iscsi.spi.RandomAccessIO;

public abstract class FileIO implements RandomAccessIO, Closeable {

  public static FileIO open(File file, int bufferSize, long length) throws IOException {
    if (isDirectIOSupported()) {
      return new DirectIO(file, bufferSize, length);
    } else {
      return new NormalIO(file, length);
    }
  }

  private static boolean isDirectIOSupported() {
    return Platform.isLinux();
  }

  @Override
  public abstract void writeFully(long position, byte[] buffer, int offset, int length) throws IOException;

  @Override
  public abstract void readFully(long position, byte[] buffer, int offset, int length) throws IOException;

  private static class DirectIO extends FileIO {

    private static final String RW = "rw";
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectIO.class);

    private final DirectRandomAccessFile _draf;
    private final ReadLock _readLock;
    private final WriteLock _writeLock;
    private final AtomicLong _position = new AtomicLong();

    public DirectIO(File file, int bufferSize, long length) throws IOException {
      try (RandomAccessFile raf = new RandomAccessFile(file, RW)) {
        raf.setLength(length);
      }
      _draf = new DirectRandomAccessFile(file, RW, bufferSize);
      ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
      _readLock = readWriteLock.readLock();
      _writeLock = readWriteLock.writeLock();
      _position.set(0);
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(LOGGER, _draf);
    }

    @Override
    public void writeFully(long position, byte[] buffer, int offset, int length) throws IOException {
      _writeLock.lock();
      try {
        seekIfNeeded(position);
        _draf.write(buffer, offset, length);
        _position.addAndGet(length);
      } finally {
        _writeLock.unlock();
      }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      _readLock.lock();
      try {
        seekIfNeeded(position);
        _draf.read(buffer, 0, length);
        _position.addAndGet(length);
      } finally {
        _readLock.unlock();
      }
    }

    private void seekIfNeeded(long position) throws IOException {
      if (_position.get() != position) {
        _draf.seek(position);
      }
    }
  }

  private static class NormalIO extends FileIO {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectIO.class);

    private final RandomAccessFile _rand;
    private final FileChannel _channel;

    public NormalIO(File file, long length) throws IOException {
      _rand = new RandomAccessFile(file, "rw");
      _rand.setLength(length);
      _channel = _rand.getChannel();
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(LOGGER, _channel, _rand);
    }

    @Override
    public void writeFully(long position, byte[] buffer, int offset, int length) throws IOException {
      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, length);
      while (byteBuffer.hasRemaining()) {
        position += _channel.write(byteBuffer, position);
      }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, length);
      while (byteBuffer.hasRemaining()) {
        position += _channel.read(byteBuffer, position);
      }
    }

  }

}
