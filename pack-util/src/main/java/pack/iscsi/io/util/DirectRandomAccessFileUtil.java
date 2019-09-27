package pack.iscsi.io.util;

import java.lang.reflect.Field;

import net.smacke.jaydio.DirectRandomAccessFile;
import net.smacke.jaydio.align.ByteChannelAligner;
import net.smacke.jaydio.channel.DirectIoByteChannel;

public class DirectRandomAccessFileUtil {
  
  private static final Field CHANNEL_DIRECT_RANDOM_ACCESS_FILE_FIELD;
  private static final Field CHANNEL_BYTE_CHANNEL_ALIGNER_FIELD;
  private static final Field FD_DIRECT_IO_BYTE_CHANNEL_FIELD;
  private static final Field FILE_LENGTH_BYTE_CHANNEL_ALIGNER_FIELD;
  private static final Field FILE_LENGTH_DIRECT_IO_BYTE_CHANNEL_FIELD;
  private static final String FILE_LENGTH = "fileLength";
  private static final String CHANNEL = "channel";
  private static final String FD = "fd";

  static {
    try {
      CHANNEL_DIRECT_RANDOM_ACCESS_FILE_FIELD = DirectRandomAccessFile.class.getDeclaredField(CHANNEL);
      CHANNEL_DIRECT_RANDOM_ACCESS_FILE_FIELD.setAccessible(true);

      CHANNEL_BYTE_CHANNEL_ALIGNER_FIELD = ByteChannelAligner.class.getDeclaredField(CHANNEL);
      CHANNEL_BYTE_CHANNEL_ALIGNER_FIELD.setAccessible(true);

      FILE_LENGTH_BYTE_CHANNEL_ALIGNER_FIELD = ByteChannelAligner.class.getDeclaredField(FILE_LENGTH);
      FILE_LENGTH_BYTE_CHANNEL_ALIGNER_FIELD.setAccessible(true);

      FD_DIRECT_IO_BYTE_CHANNEL_FIELD = DirectIoByteChannel.class.getDeclaredField(FD);
      FD_DIRECT_IO_BYTE_CHANNEL_FIELD.setAccessible(true);

      FILE_LENGTH_DIRECT_IO_BYTE_CHANNEL_FIELD = DirectIoByteChannel.class.getDeclaredField(FILE_LENGTH);
      FILE_LENGTH_DIRECT_IO_BYTE_CHANNEL_FIELD.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static int getDescriptor(DirectRandomAccessFile directRandomAccessFile) {
    try {
      Object byteChannelAligner = CHANNEL_DIRECT_RANDOM_ACCESS_FILE_FIELD.get(directRandomAccessFile);
      Object directIoByteChannel = CHANNEL_BYTE_CHANNEL_ALIGNER_FIELD.get(byteChannelAligner);
      return (int) FD_DIRECT_IO_BYTE_CHANNEL_FIELD.get(directIoByteChannel);
    } catch (Exception e) {
      throw new UnsupportedOperationException("unsupported DirectRandomAccessFile implementation", e);
    }
  }

  public static void setLength(DirectRandomAccessFile directRandomAccessFile, long length) {
    try {

      Object byteChannelAligner = CHANNEL_DIRECT_RANDOM_ACCESS_FILE_FIELD.get(directRandomAccessFile);
      FILE_LENGTH_BYTE_CHANNEL_ALIGNER_FIELD.set(byteChannelAligner, length);

      Object directIoByteChannel = CHANNEL_BYTE_CHANNEL_ALIGNER_FIELD.get(byteChannelAligner);
      FILE_LENGTH_DIRECT_IO_BYTE_CHANNEL_FIELD.set(directIoByteChannel, length);

    } catch (Exception e) {
      throw new UnsupportedOperationException("unsupported DirectRandomAccessFile implementation", e);
    }
  }

  public static void flush(DirectRandomAccessFile directRandomAccessFile) {
    try {
      ByteChannelAligner<?> byteChannelAligner = (ByteChannelAligner<?>) CHANNEL_DIRECT_RANDOM_ACCESS_FILE_FIELD.get(
          directRandomAccessFile);
      byteChannelAligner.flush();
    } catch (Exception e) {
      throw new UnsupportedOperationException("unsupported DirectRandomAccessFile implementation", e);
    }
  }
}
