package pack.iscsi.io;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;

import com.sun.jna.Native;
import com.sun.jna.Platform;

import net.smacke.jaydio.DirectRandomAccessFile;
import net.smacke.jaydio.align.ByteChannelAligner;
import net.smacke.jaydio.channel.DirectIoByteChannel;

public final class Fallocate {

  private static final boolean IS_LINUX = Platform.isLinux();
  private static final boolean IS_POSIX = !Platform.isWindows();

  private static final int FALLOC_FL_KEEP_SIZE = 0x01;

  private final int fd;
  private int mode;
  private long offset;
  private final long length;

  private Fallocate(int fd, long length) {
    if (!isSupported()) {
      throwUnsupported("fallocate");
    }
    this.fd = fd;
    this.length = length;
  }

  public static boolean isSupported() {
    return IS_POSIX;
  }

  public static Fallocate forDirectRandomAccessFile(DirectRandomAccessFile directRandomAccessFile, long length) {
    return new Fallocate(getDescriptor(directRandomAccessFile), length);
  }

  public static Fallocate forChannel(FileChannel channel, long length) {
    return new Fallocate(getDescriptor(channel), length);
  }

  public static Fallocate forDescriptor(FileDescriptor descriptor, long length) {
    return new Fallocate(getDescriptor(descriptor), length);
  }

  public Fallocate fromOffset(long offset) {
    this.offset = offset;
    return this;
  }

  public Fallocate keepSize() {
    requireLinux("fallocate keep size");
    mode |= FALLOC_FL_KEEP_SIZE;
    return this;
  }

  private void requireLinux(String feature) {
    if (!IS_LINUX) {
      throwUnsupported(feature);
    }
  }

  private void throwUnsupported(String feature) {
    throw new UnsupportedOperationException(feature + " is not supported on this operating system");
  }

  public void execute() throws IOException {
    final int errno;
    if (IS_LINUX) {
      final int result = FallocateHolder.fallocate(fd, mode, offset, length);
      errno = result == 0 ? 0 : Native.getLastError();
    } else {
      errno = PosixFallocateHolder.posix_fallocate(fd, offset, length);
    }
    if (errno != 0) {
      throw new IOException("fallocate returned " + errno);
    }
  }

  private static class FallocateHolder {
    static {
      Native.register(Platform.C_LIBRARY_NAME);
    }

    private static native int fallocate(int fd, int mode, long offset, long length);
  }

  private static class PosixFallocateHolder {
    static {
      Native.register(Platform.C_LIBRARY_NAME);
    }

    private static native int posix_fallocate(int fd, long offset, long length);
  }

  private static int getDescriptor(DirectRandomAccessFile directRandomAccessFile) {
    try {
      Object directIoByteChannelAligner = getValue(DirectRandomAccessFile.class, directRandomAccessFile, "channel");
      if (directIoByteChannelAligner == null) {
        throw new UnsupportedOperationException("unsupported DirectRandomAccessFile implementation");
      }
      Object byteChannelAligner = getValue(ByteChannelAligner.class, directIoByteChannelAligner, "channel");
      if (byteChannelAligner == null) {
        throw new UnsupportedOperationException("unsupported DirectRandomAccessFile implementation");
      }
      Object fd = getValue(DirectIoByteChannel.class, byteChannelAligner, "fd");
      if (fd == null) {
        throw new UnsupportedOperationException("unsupported DirectRandomAccessFile implementation");
      }
      return (Integer) fd;
    } catch (final Exception e) {
      throw new UnsupportedOperationException("unsupported DirectRandomAccessFile implementation", e);
    }
  }

  private static Object getValue(Class<?> clazz, Object obj, String name) throws Exception {
    Field field = clazz.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(obj);
  }

  private static int getDescriptor(FileChannel channel) {
    try {
      final Field field = channel.getClass()
                                 .getDeclaredField("fd");
      field.setAccessible(true);
      return getDescriptor((FileDescriptor) field.get(channel));
    } catch (final Exception e) {
      throw new UnsupportedOperationException("unsupported FileChannel implementation", e);
    }
  }

  private static int getDescriptor(FileDescriptor descriptor) {
    try {
      final Field field = descriptor.getClass()
                                    .getDeclaredField("fd");
      field.setAccessible(true);
      return (int) field.get(descriptor);
    } catch (final Exception e) {
      throw new UnsupportedOperationException("unsupported FileDescriptor implementation", e);
    }
  }
}
