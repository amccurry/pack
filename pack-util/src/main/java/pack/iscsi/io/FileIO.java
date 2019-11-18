package pack.iscsi.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.sun.jna.Platform;

import pack.iscsi.io.direct.DirectIO;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.RandomAccessIOReader;

public abstract class FileIO implements RandomAccessIO {

  private static boolean _directIOEnabled = true;

  public static RandomAccessIO openRandomAccess(File file, int blockSize, String mode) throws IOException {
    return openRandomAccess(file, blockSize, mode, _directIOEnabled);
  }

  public static RandomAccessIO openRandomAccess(File file, int blockSize, String mode, boolean direct)
      throws IOException {
    if (isDirectIOSupported() && direct) {
      return new DirectIO(file);
    } else {
      return new FileIORandomAccessFile(new RandomAccessFile(file, mode));
    }
  }

  private static boolean isDirectIOSupported() {
    return isDirectIOEnabled() && Platform.isLinux();
  }

  @Override
  public abstract void write(long position, byte[] buffer, int offset, int length) throws IOException;

  @Override
  public abstract void read(long position, byte[] buffer, int offset, int length) throws IOException;

  private static class FileIORandomAccessFile implements RandomAccessIO {

    private final RandomAccessFile _raf;
    private final FileChannel _channel;

    public FileIORandomAccessFile(RandomAccessFile raf) {
      _raf = raf;
      _channel = _raf.getChannel();
    }

    @Override
    public void write(long position, byte[] buffer, int offset, int length) throws IOException {
      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, length);
      while (byteBuffer.hasRemaining()) {
        position += _channel.write(byteBuffer, position);
      }
    }

    @Override
    public void read(long position, byte[] buffer, int offset, int length) throws IOException {
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

  }

  public static void setDirectIOEnabled(boolean directIOEnabled) {
    _directIOEnabled = directIOEnabled;
  }

  public static boolean isDirectIOEnabled() {
    return _directIOEnabled;
  }

}
