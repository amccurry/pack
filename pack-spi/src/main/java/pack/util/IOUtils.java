package pack.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IOUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(IOUtils.class);

  public static int getBlockOffset(long position, int blockSize) {
    return (int) (position % blockSize);
  }

  public static long getBlockId(long position, int blockSize) {
    return position / blockSize;
  }

  public static void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

  public static void closeQuietly(Future<?>... futures) {
    if (futures == null) {
      return;
    }
    for (Future<?> future : futures) {
      if (future != null) {
        future.cancel(true);
      }
    }
  }

  public static void closeQuietly(Closeable... closeables) {
    close(null, closeables);
  }

  public static void close(Logger logger, Closeable... closeables) {
    if (closeables == null) {
      return;
    }
    for (Closeable closeable : closeables) {
      if (closeable != null) {
        try {
          closeable.close();
        } catch (IOException e) {
          if (logger != null) {
            LOGGER.error("Unknown error while trying to close " + closeable, e);
          }
        }
      }
    }
  }

}
