package pack.iscsi.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Platform;

import io.opentracing.Scope;
import net.smacke.jaydio.DirectRandomAccessFile;
import net.smacke.jaydio.align.DirectIoByteChannelAligner;
import pack.iscsi.io.util.DirectRandomAccessFileUtil;
import pack.iscsi.io.util.NativeFileUtil;
import pack.iscsi.io.util.NativeFileUtil.FallocateMode;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.RandomAccessIOReader;
import pack.util.tracer.TracerUtil;

public abstract class FileIO implements RandomAccessIO {

  private static boolean _directIOEnabled = true;

  public static RandomAccessIO openRandomAccess(File file, int bufferSize, String mode) throws IOException {
    return openRandomAccess(file, bufferSize, mode, _directIOEnabled);
  }

  public static RandomAccessIO openRandomAccess(File file, int bufferSize, String mode, boolean direct)
      throws IOException {
    if (isDirectIOSupported() && direct) {

      return new FileIODirectRandomAccessFile(file, new DirectRandomAccessFile(file, mode));

      // return new DirectIO(file);

    } else {
      return new FileIORandomAccessFile(new RandomAccessFile(file, mode));
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
      try (Scope scope1 = TracerUtil.trace(FileIODirectRandomAccessFile.class, "readFully")) {
        synchronized (_lock) {
          seekIfNeeded(position);
          try (Scope scope2 = TracerUtil.trace(FileIODirectRandomAccessFile.class, "read")) {
            _draf.read(buffer, offset, length);
          }
        }
      }
    }

    @Override
    public void punchHole(long position, long length) throws IOException {
      synchronized (_lock) {
        if (Platform.isLinux()) {
          NativeFileUtil.fallocate(_draf, position, length, FallocateMode.FALLOC_FL_PUNCH_HOLE,
              FallocateMode.FALLOC_FL_KEEP_SIZE);
        }
      }
    }

    @Override
    public long length() throws IOException {
      synchronized (_lock) {
        return _draf.length();
      }
    }

    @Override
    public void close() throws IOException {
      synchronized (_lock) {
        LOGGER.info("Closing file {}", _file);
        _draf.close();
      }
    }

    @Override
    public RandomAccessIOReader cloneReadOnly() throws IOException {
      synchronized (_lock) {
        DirectIoByteChannelAligner channel = getChannel(_draf);
        channel.flush();
        return new FileIODirectRandomAccessFileReader(_file);
      }
    }

    @Override
    public void setLength(long length) throws IOException {
      synchronized (_lock) {
        NativeFileUtil.ftruncate(_draf, length);
      }
    }

    @Override
    public void flush() throws IOException {
      synchronized (_lock) {
        DirectRandomAccessFileUtil.flush(_draf);
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

    private static final Logger LOGGER = LoggerFactory.getLogger(FileIODirectRandomAccessFileReader.class);

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
      LOGGER.debug("Closing file {}", _file);
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

    @Override
    public void setLength(long length) throws IOException {
      _raf.setLength(length);
    }

    @Override
    public void flush() throws IOException {

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
