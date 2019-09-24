package pack.iscsi.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Platform;

import io.opencensus.common.Scope;
import net.smacke.jaydio.DirectRandomAccessFile;
import pack.iscsi.spi.RandomAccessIO;
import pack.util.TracerUtil;

public abstract class FileIO implements RandomAccessIO {

  private static final String RW = "rw";
  private static boolean _directIOEnabled = true;

  public static void setLengthFile(File file, long length) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(file, RW)) {
      raf.setLength(length);
    }
  }

  public static RandomAccessIO openRandomAccess(File file, int bufferSize, String mode) throws IOException {
    if (isDirectIOSupported()) {
      return new FileIODirectRandomAccessFile(file, new DirectRandomAccessFile(file, mode));
    } else {
      return new FileIORandomAccessFile(new RandomAccessFile(file, mode));
    }
  }

  public static InputStream openStream(File file, int bufferSize) throws IOException {
    if (isDirectIOSupported()) {
      return new DirectInputStream(file, bufferSize);
    } else {
      return new FileInputStream(file);
    }
  }

  public static OutputStream createStream(File file, int bufferSize) throws IOException {
    if (isDirectIOSupported()) {
      return new DirectOutputStream(file, bufferSize);
    } else {
      return new FileOutputStream(file);
    }
  }

  private static boolean isDirectIOSupported() {
    return isDirectIOEnabled() && Platform.isLinux();
  }

  @Override
  public abstract void writeFully(long position, byte[] buffer, int offset, int length) throws IOException;

  @Override
  public abstract void readFully(long position, byte[] buffer, int offset, int length) throws IOException;

  private static class FileIODirectRandomAccessFile implements RandomAccessIO {

    private final DirectRandomAccessFile _draf;
    private final ReadLock _readLock;
    private final WriteLock _writeLock;

    public FileIODirectRandomAccessFile(File file, DirectRandomAccessFile draf) {
      _draf = draf;
      ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
      _readLock = readWriteLock.readLock();
      _writeLock = readWriteLock.writeLock();
    }

    @Override
    public void writeFully(long position, byte[] buffer, int offset, int length) throws IOException {
      try (Scope scope1 = TracerUtil.trace(FileIODirectRandomAccessFile.class, "writeFully")) {
        _writeLock.lock();
        try {
          seekIfNeeded(position);
          try (Scope scope2 = TracerUtil.trace(FileIODirectRandomAccessFile.class, "write")) {
            _draf.write(buffer, offset, length);
          }
        } finally {
          _writeLock.unlock();
        }
      }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      _readLock.lock();
      try {
        seekIfNeeded(position);
        _draf.read(buffer, offset, length);
      } finally {
        _readLock.unlock();
      }
    }

    private void seekIfNeeded(long position) throws IOException {
      try (Scope scope = TracerUtil.trace(FileIODirectRandomAccessFile.class, "seekIfNeeded")) {
        if (_draf.getFilePointer() != position) {
          _draf.seek(position);
        }
      }
    }

    @Override
    public void seek(long position) throws IOException {
      _draf.seek(position);
    }

    @Override
    public long getFilePointer() throws IOException {
      return _draf.getFilePointer();
    }

    @Override
    public long length() throws IOException {
      return _draf.length();
    }

    @Override
    public int read() throws IOException {
      return _draf.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      _draf.read(b, off, len);
      return len;
    }

    @Override
    public int read(byte[] b) throws IOException {
      _draf.read(b);
      return b.length;
    }

    @Override
    public void close() throws IOException {
      _draf.close();
    }

    @Override
    public void write(byte[] b) throws IOException {
      try (Scope scope = TracerUtil.trace(FileIODirectRandomAccessFile.class, "write")) {
        _draf.write(b);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      try (Scope scope = TracerUtil.trace(FileIODirectRandomAccessFile.class, "write")) {
        _draf.write(b, off, len);
      }
    }

    @Override
    public void readFully(byte[] b) throws IOException {
      _draf.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
      _draf.readFully(b, off, len);
    }

    @Override
    public int readInt() throws IOException {
      return _draf.readInt();
    }

    @Override
    public long readLong() throws IOException {
      return _draf.readLong();
    }
  }

  private static class FileIORandomAccessFile implements RandomAccessIO {

    private final RandomAccessFile _raf;
    private final FileChannel _channel;

    public FileIORandomAccessFile(RandomAccessFile raf) {
      _raf = raf;
      _channel = _raf.getChannel();
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

    @Override
    public void close() throws IOException {
      _channel.close();
      _raf.close();
    }

    @Override
    public int read() throws IOException {
      return _raf.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return _raf.read(b, off, len);
    }

    @Override
    public int read(byte[] b) throws IOException {
      return _raf.read(b);
    }

    @Override
    public final void readFully(byte[] b) throws IOException {
      _raf.readFully(b);
    }

    @Override
    public final void readFully(byte[] b, int off, int len) throws IOException {
      _raf.readFully(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
      _raf.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      _raf.write(b, off, len);
    }

    @Override
    public long getFilePointer() throws IOException {
      return _raf.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
      _raf.seek(pos);
    }

    @Override
    public long length() throws IOException {
      return _raf.length();
    }

    @Override
    public final int readInt() throws IOException {
      return _raf.readInt();
    }

    @Override
    public final long readLong() throws IOException {
      return _raf.readLong();
    }

  }

  private static class DirectInputStream extends InputStream {

    private static final String R = "r";
    private final DirectRandomAccessFile _draf;

    public DirectInputStream(File file, int bufferSize) throws IOException {
      _draf = new DirectRandomAccessFile(file, R, bufferSize);
    }

    @Override
    public int read() throws IOException {
      return _draf.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      _draf.read(b, off, len);
      return len;
    }

    @Override
    public void close() throws IOException {
      _draf.close();
    }

  }

  private static class DirectOutputStream extends OutputStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectOutputStream.class);

    private final DirectRandomAccessFile _draf;

    public DirectOutputStream(File file, int bufferSize) throws IOException {
      try (RandomAccessFile raf = new RandomAccessFile(file, RW)) {
        raf.setLength(0);
      }
      _draf = new DirectRandomAccessFile(file, RW, bufferSize);
    }

    @Override
    public void write(int b) throws IOException {
      _draf.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      _draf.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(LOGGER, _draf);
    }
  }

  public static void setDirectIOEnabled(boolean directIOEnabled) {
    _directIOEnabled = directIOEnabled;
  }

  public static boolean isDirectIOEnabled() {
    return _directIOEnabled;
  }
}
