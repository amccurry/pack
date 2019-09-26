package pack.iscsi.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Platform;

import io.opencensus.common.Scope;
import net.smacke.jaydio.DirectRandomAccessFile;
import net.smacke.jaydio.align.DirectIoByteChannelAligner;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.RandomAccessIOReader;
import pack.util.TracerUtil;

public abstract class FileIO implements RandomAccessIO {

  private static final String RW = "rw";
  private static boolean _directIOEnabled = true;

  public static void setLengthFile(File file, long length) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(file, RW)) {
      raf.setLength(length);
    }
  }

  public static void setSparseLengthFile(File file, long length) throws IOException {
    setLengthFile(file, length);
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

    private static final Logger LOGGER = LoggerFactory.getLogger(FileIODirectRandomAccessFile.class);

    private final DirectRandomAccessFile _draf;
    private final File _file;
    private final Object _lock = new Object();

    public FileIODirectRandomAccessFile(File file, DirectRandomAccessFile draf) {
      _draf = draf;
      _file = file;
    }

    @Override
    public void writeFully(long position, byte[] buffer, int offset, int length) throws IOException {
      try (Scope scope1 = TracerUtil.trace(FileIODirectRandomAccessFile.class, "writeFully")) {
        synchronized (_lock) {
          seekIfNeeded(position);
          try (Scope scope2 = TracerUtil.trace(FileIODirectRandomAccessFile.class, "write")) {
            _draf.write(buffer, offset, length);
          }
        }
      }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      synchronized (_lock) {
        seekIfNeeded(position);
        _draf.read(buffer, offset, length);
      }
    }

    @Override
    public long length() throws IOException {
      return _draf.length();
    }

    @Override
    public void close() throws IOException {
      LOGGER.info("Closing FileIODirectRandomAccessFile file {}", _file);
      _draf.close();
    }

    @Override
    public RandomAccessIOReader cloneReadOnly() throws IOException {
      synchronized (_lock) {
        DirectIoByteChannelAligner channel = getChannel(_draf);
        channel.flush();
        return new FileIODirectRandomAccessFileReader(_file);
      }
    }

    private void seekIfNeeded(long position) throws IOException {
      try (Scope scope = TracerUtil.trace(FileIODirectRandomAccessFile.class, "seekIfNeeded")) {
        if (_draf.getFilePointer() != position) {
          _draf.seek(position);
        }
      }
    }

  }

  private static class FileIODirectRandomAccessFileReader implements RandomAccessIOReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileIODirectRandomAccessFile.class);

    private final DirectRandomAccessFile _draf;
    private final File _file;
    private final Object _lock = new Object();

    public FileIODirectRandomAccessFileReader(File file) throws IOException {
      _draf = new DirectRandomAccessFile(file, "r");
      _file = file;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      synchronized (_lock) {
        seekIfNeeded(position);
        _draf.read(buffer, offset, length);
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
    public long length() throws IOException {
      return _draf.length();
    }

    @Override
    public void close() throws IOException {
      LOGGER.info("Closing FileIODirectRandomAccessFile file {}", _file);
      _draf.close();
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
    public long length() throws IOException {
      return _raf.length();
    }

    @Override
    public RandomAccessIOReader cloneReadOnly() throws IOException {
      return this;
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

  private static DirectIoByteChannelAligner getChannel(DirectRandomAccessFile draf) throws IOException {
    try {
      Field field = DirectRandomAccessFile.class.getDeclaredField("channel");
      field.setAccessible(true);
      return (DirectIoByteChannelAligner) field.get(draf);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
