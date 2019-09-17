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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Platform;

import net.smacke.jaydio.DirectRandomAccessFile;
import net.smacke.jaydio.align.ByteChannelAligner;
import net.smacke.jaydio.channel.DirectIoByteChannel;
import pack.iscsi.spi.RandomAccessIO;

public abstract class FileIO implements RandomAccessIO {

  private static final String RW = "rw";

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
    return Platform.isLinux();
  }

  @Override
  public abstract void writeFully(long position, byte[] buffer, int offset, int length) throws IOException;

  @Override
  public abstract void readFully(long position, byte[] buffer, int offset, int length) throws IOException;

  private static ByteChannelAligner<?> getByteChannelAligner(DirectRandomAccessFile directRandomAccessFile) {
    try {
      return (ByteChannelAligner<?>) getValue(DirectRandomAccessFile.class, directRandomAccessFile, "channel");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static DirectIoByteChannel getDirectIoByteChannel(ByteChannelAligner<?> byteChannelAligner) {
    try {
      return (DirectIoByteChannel) getValue(ByteChannelAligner.class, byteChannelAligner, "channel");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Object getValue(Class<?> clazz, Object obj, String name) throws Exception {
    Field field = clazz.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(obj);
  }

  private static void setValue(Class<?> clazz, Object obj, String name, Object value) throws Exception {
    Field field = clazz.getDeclaredField(name);
    field.setAccessible(true);
    field.set(obj, value);
  }

  private static class FileIODirectRandomAccessFile implements RandomAccessIO {

    private static final String FILE_LENGTH = "fileLength";
    private final DirectRandomAccessFile _draf;
    private final ReadLock _readLock;
    private final WriteLock _writeLock;
    private final ByteChannelAligner<?> _byteChannelAligner;
    private final DirectIoByteChannel _directIoByteChannel;

    public FileIODirectRandomAccessFile(File file, DirectRandomAccessFile draf) {
      _draf = draf;
      _byteChannelAligner = getByteChannelAligner(_draf);
      _directIoByteChannel = getDirectIoByteChannel(_byteChannelAligner);
      ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
      _readLock = readWriteLock.readLock();
      _writeLock = readWriteLock.writeLock();
    }

    @Override
    public void writeFully(long position, byte[] buffer, int offset, int length) throws IOException {
      _writeLock.lock();
      try {
        seekIfNeeded(position);
        _draf.write(buffer, offset, length);
      } finally {
        _writeLock.unlock();
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
      if (_draf.getFilePointer() != position) {
        _draf.seek(position);
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
    public void setLength(long newLength) throws IOException {
      Fallocate fallocate = Fallocate.forDirectRandomAccessFile(_draf, newLength);
      fallocate.execute();
      try {
        setValue(DirectIoByteChannel.class, _directIoByteChannel, FILE_LENGTH, newLength);
      } catch (Exception e) {
        throw new IOException(e);
      }
      try {
        setValue(ByteChannelAligner.class, _byteChannelAligner, FILE_LENGTH, newLength);
      } catch (Exception e) {
        throw new IOException(e);
      }
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
    public void write(int b) throws IOException {
      _draf.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      _draf.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      _draf.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
      _draf.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
      _draf.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
      _draf.writeShort(v);
    }

    @Override
    public void readFully(byte[] b) throws IOException {
      _draf.readFully(b);
    }

    @Override
    public void writeChar(int v) throws IOException {
      _draf.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
      _draf.writeInt(v);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
      _draf.readFully(b, off, len);
    }

    @Override
    public void writeLong(long v) throws IOException {
      _draf.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
      _draf.writeFloat(v);
    }

    @Override
    public int skipBytes(int n) throws IOException {
      return _draf.skipBytes(n);
    }

    @Override
    public void writeDouble(double v) throws IOException {
      _draf.writeDouble(v);
    }

    @Override
    public boolean readBoolean() throws IOException {
      return _draf.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
      return _draf.readByte();
    }

    @Override
    public void writeBytes(String s) throws IOException {
      _draf.writeBytes(s);
    }

    @Override
    public int readUnsignedByte() throws IOException {
      return _draf.readUnsignedByte();
    }

    @Override
    public void writeChars(String s) throws IOException {
      _draf.writeChars(s);
    }

    @Override
    public short readShort() throws IOException {
      return _draf.readShort();
    }

    @Override
    public void writeUTF(String s) throws IOException {
      _draf.writeUTF(s);
    }

    @Override
    public int readUnsignedShort() throws IOException {
      return _draf.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
      return _draf.readChar();
    }

    @Override
    public int readInt() throws IOException {
      return _draf.readInt();
    }

    @Override
    public long readLong() throws IOException {
      return _draf.readLong();
    }

    public float readFloat() throws IOException {
      return _draf.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
      return _draf.readDouble();
    }

    @Override
    public String readLine() throws IOException {
      return _draf.readLine();
    }

    @Override
    public String readUTF() throws IOException {
      return _draf.readUTF();
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
    public int skipBytes(int n) throws IOException {
      return _raf.skipBytes(n);
    }

    @Override
    public void write(int b) throws IOException {
      _raf.write(b);
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
    public void setLength(long newLength) throws IOException {
      _raf.setLength(newLength);
    }

    @Override
    public final boolean readBoolean() throws IOException {
      return _raf.readBoolean();
    }

    @Override
    public final byte readByte() throws IOException {
      return _raf.readByte();
    }

    @Override
    public final int readUnsignedByte() throws IOException {
      return _raf.readUnsignedByte();
    }

    @Override
    public final short readShort() throws IOException {
      return _raf.readShort();
    }

    @Override
    public final int readUnsignedShort() throws IOException {
      return _raf.readUnsignedShort();
    }

    @Override
    public final char readChar() throws IOException {
      return _raf.readChar();
    }

    @Override
    public final int readInt() throws IOException {
      return _raf.readInt();
    }

    @Override
    public final long readLong() throws IOException {
      return _raf.readLong();
    }

    @Override
    public final float readFloat() throws IOException {
      return _raf.readFloat();
    }

    @Override
    public final double readDouble() throws IOException {
      return _raf.readDouble();
    }

    @Override
    public final String readLine() throws IOException {
      return _raf.readLine();
    }

    @Override
    public final String readUTF() throws IOException {
      return _raf.readUTF();
    }

    @Override
    public final void writeBoolean(boolean v) throws IOException {
      _raf.writeBoolean(v);
    }

    @Override
    public final void writeByte(int v) throws IOException {
      _raf.writeByte(v);
    }

    @Override
    public final void writeShort(int v) throws IOException {
      _raf.writeShort(v);
    }

    @Override
    public final void writeChar(int v) throws IOException {
      _raf.writeChar(v);
    }

    @Override
    public final void writeInt(int v) throws IOException {
      _raf.writeInt(v);
    }

    @Override
    public final void writeLong(long v) throws IOException {
      _raf.writeLong(v);
    }

    @Override
    public final void writeFloat(float v) throws IOException {
      _raf.writeFloat(v);
    }

    @Override
    public final void writeDouble(double v) throws IOException {
      _raf.writeDouble(v);
    }

    @Override
    public final void writeBytes(String s) throws IOException {
      _raf.writeBytes(s);
    }

    @Override
    public final void writeChars(String s) throws IOException {
      _raf.writeChars(s);
    }

    @Override
    public final void writeUTF(String str) throws IOException {
      _raf.writeUTF(str);
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
}
