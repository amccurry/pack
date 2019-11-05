package pack.iscsi.io.util;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Native;
import com.sun.jna.Platform;

import net.smacke.jaydio.DirectRandomAccessFile;

public class NativeFileUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeFileUtil.class);

  private static final String FD = "fd";

  private static class FtruncateHolder {
    static {
      Native.register(Platform.C_LIBRARY_NAME);
    }

    private static native int ftruncate(int fd, long length);
  }

  private static class FallocateHolder {
    static {
      Native.register(Platform.C_LIBRARY_NAME);
    }

    private static native int fallocate(int fd, int mode, long offset, long length);
  }

  public static void ftruncate(File file, long length) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      ftruncate(raf.getFD(), length);
    }
  }

  public static void ftruncate(RandomAccessFile raf, long length) throws IOException {
    ftruncate(raf.getFD(), length);
  }

  public static void ftruncate(FileDescriptor fileDescriptor, long length) throws IOException {
    int fd = getDescriptor(fileDescriptor);
    ftruncate(fd, length);
  }

  public static void ftruncate(DirectRandomAccessFile directRandomAccessFile, long length) throws IOException {
    int fd = DirectRandomAccessFileUtil.getDescriptor(directRandomAccessFile);
    ftruncate(fd, length);
    DirectRandomAccessFileUtil.setLength(directRandomAccessFile, length);
  }

  public static void ftruncate(int fd, long length) throws IOException {
    int result = FtruncateHolder.ftruncate(fd, length);
    if (result != 0) {
      throw new IOException("fallocate returned " + Native.getLastError());
    }
  }

  public static void fallocate(File file, long offset, long length, FallocateMode... modeArray) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      fallocate(raf.getFD(), offset, length, modeArray);
    }
  }

  public static void fallocate(RandomAccessFile raf, long offset, long length, FallocateMode... modeArray)
      throws IOException {
    fallocate(raf.getFD(), offset, length, modeArray);
  }

  public static void fallocate(FileDescriptor fileDescriptor, long offset, long length, FallocateMode... modeArray)
      throws IOException {
    int fd = getDescriptor(fileDescriptor);
    fallocate(fd, offset, length, modeArray);
  }

  public static void fallocate(DirectRandomAccessFile directRandomAccessFile, long offset, long length,
      FallocateMode... modeArray) throws IOException {
    int fd = DirectRandomAccessFileUtil.getDescriptor(directRandomAccessFile);
    fallocate(fd, offset, length, modeArray);
  }

  public static void fallocate(int fd, long offset, long length, FallocateMode... modeArray) throws IOException {
    int mode = getMode(modeArray);
    LOGGER.debug("fallocate fd {} mode {}({}) offset {} length {}", fd, Arrays.asList(modeArray), mode, offset, length);
    int result = FallocateHolder.fallocate(fd, mode, offset, length);
    if (result != 0) {
      throw new IOException("fallocate returned " + Native.getLastError());
    }
  }

  private static int getMode(FallocateMode... modeArray) {
    int mode = 0;
    if (modeArray == null || modeArray.length == 0) {
      return mode;
    }
    for (FallocateMode fallocateMode : modeArray) {
      mode = (mode | fallocateMode.getValue());
    }
    return mode;
  }

  public static void fallocate(int fd, int mode, long offset, long length) throws IOException {
    int result = FallocateHolder.fallocate(fd, mode, offset, length);
    if (result != 0) {
      throw new IOException("fallocate returned " + Native.getLastError());
    }
  }

  private static int getDescriptor(FileDescriptor descriptor) {
    try {
      final Field field = descriptor.getClass()
                                    .getDeclaredField(FD);
      field.setAccessible(true);
      return (int) field.get(descriptor);
    } catch (final Exception e) {
      throw new UnsupportedOperationException("unsupported FileDescriptor implementation", e);
    }
  }

  public enum FallocateMode {

    /* default is extend size */
    FALLOC_FL_KEEP_SIZE(0x01),

    /* de-allocates range */
    FALLOC_FL_PUNCH_HOLE(0x02),

    /* reserved codepoint */
    FALLOC_FL_NO_HIDE_STALE(0x04),

    /*
     * FALLOC_FL_COLLAPSE_RANGE is used to remove a range of a file without
     * leaving a hole in the file. The contents of the file beyond the range
     * being removed is appended to the start offset of the range being removed
     * (i.e. the hole that was punched is "collapsed"), resulting in a file
     * layout that looks like the range that was removed never existed. As such
     * collapsing a range of a file changes the size of the file, reducing it by
     * the same length of the range that has been removed by the operation.
     *
     * Different filesystems may implement different limitations on the
     * granularity of the operation. Most will limit operations to filesystem
     * block size boundaries, but this boundary may be larger or smaller
     * depending on the filesystem and/or the configuration of the filesystem or
     * file.
     *
     * Attempting to collapse a range that crosses the end of the file is
     * considered an illegal operation - just use ftruncate(2) if you need to
     * collapse a range that crosses EOF.
     */
    FALLOC_FL_COLLAPSE_RANGE(0x08),

    /*
     * FALLOC_FL_ZERO_RANGE is used to convert a range of file to zeros
     * preferably without issuing data IO. Blocks should be preallocated for the
     * regions that span holes in the file, and the entire range is preferable
     * converted to unwritten extents - even though file system may choose to
     * zero out the extent or do whatever which will result in reading zeros
     * from the range while the range remains allocated for the file.
     *
     * This can be also used to preallocate blocks past EOF in the same way as
     * with fallocate. Flag FALLOC_FL_KEEP_SIZE should cause the inode size to
     * remain the same.
     */

    FALLOC_FL_ZERO_RANGE(0x10),

    /*
     * FALLOC_FL_INSERT_RANGE is use to insert space within the file size
     * without overwriting any existing data. The contents of the file beyond
     * offset are shifted towards right by len bytes to create a hole. As such,
     * this operation will increase the size of the file by len bytes.
     *
     * Different filesystems may implement different limitations on the
     * granularity of the operation. Most will limit operations to filesystem
     * block size boundaries, but this boundary may be larger or smaller
     * depending on the filesystem and/or the configuration of the filesystem or
     * file.
     *
     * Attempting to insert space using this flag at OR beyond the end of the
     * file is considered an illegal operation - just use ftruncate(2) or
     * fallocate(2) with mode 0 for such type of operations.
     */
    FALLOC_FL_INSERT_RANGE(0x20),

    /*
     * FALLOC_FL_UNSHARE_RANGE is used to unshare shared blocks within the file
     * size without overwriting any existing data. The purpose of this call is
     * to preemptively reallocate any blocks that are subject to copy-on-write.
     *
     * Different filesystems may implement different limitations on the
     * granularity of the operation. Most will limit operations to filesystem
     * block size boundaries, but this boundary may be larger or smaller
     * depending on the filesystem and/or the configuration of the filesystem or
     * file.
     *
     * This flag can only be used with allocate-mode fallocate, which is to say
     * that it cannot be used with the punch, zero, collapse, or insert range
     * modes.
     */
    FALLOC_FL_UNSHARE_RANGE(0x40);

    private final int _value;

    private FallocateMode(int value) {
      _value = value;
    }

    public int getValue() {
      return _value;
    }
  }
}
