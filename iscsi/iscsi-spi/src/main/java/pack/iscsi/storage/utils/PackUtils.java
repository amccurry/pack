package pack.iscsi.storage.utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

public class PackUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackUtils.class);

  public static void closeQuietly(Closeable... closeables) {
    if (closeables == null) {
      return;
    }
    for (Closeable closeable : closeables) {
      if (closeable != null) {
        try {
          closeable.close();
        } catch (IOException e) {

        }
      }
    }
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
          logger.error("Unknown error", e);
        }
      }
    }
  }

  public static void checkFutureIsRunning(Future<Void> future) {
    if (future == null) {
      return;
    }
    if (future.isDone()) {
      try {
        future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException("Future " + future + " failed");
      } catch (ExecutionException e) {
        Throwable throwable = e.getCause();
        throw (RuntimeException) throwable;
      }
    }
  }

  public static int getBlockOffset(long position, int blockSize) {
    return (int) (position % blockSize);
  }

  public static long getBlockId(long position, int blockSize) {
    return position / blockSize;
  }

  public static void assertIsValidForWriting(long storageIndex, int length, int blockSize) throws IOException {
    int blockOffset = getBlockOffset(storageIndex, blockSize);
    if (blockOffset != 0) {
      LOGGER.error("storage index {} is invalid produced blockOffset of {} with blockSize set to {}", storageIndex,
          blockOffset, blockSize);
      throw new IOException("storage index " + storageIndex + " is invalid produced blockOffset of " + blockOffset
          + " with blockSize set to " + blockSize);
    }
    if (length % blockSize != 0) {
      LOGGER.error("block length {} is invalid with blockSize set to {}", length, blockSize);
      throw new IOException("block length " + length + " is invalid with blockSize set to " + blockSize);
    }
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

  public static String getEnvFailIfMissing(String name) {
    String value = getEnv(name, null);
    if (value == null) {
      throw new RuntimeException("required ENV var " + name + " missing");
    }
    return value;
  }

  public static String getEnv(String name) {
    return getEnv(name, null);
  }

  public static String getEnv(String name, String defaultValue) {
    String value = System.getenv(name);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  public static List<String> getEnvList(String name) {
    String value = getEnv(name);
    if (value == null) {
      return null;
    }
    return Splitter.on(',')
                   .splitToList(value);
  }

  public static List<String> getEnvListFailIfMissing(String name) {
    List<String> list = getEnvList(name);
    if (list == null) {
      throw new RuntimeException("required ENV var " + name + " missing");
    }
    return list;
  }

  public static boolean isEnvSet(String name) {
    return System.getenv(name) != null;
  }

  public static String getTopic(String name, String id) {
    return "pack." + name + "." + id;
  }

  public static long getPosition(int blockId, int blockSize) {
    return (long) blockId * (long) blockSize;
  }

  public static String toMd5(byte[] value) {
    try {
      return new BigInteger(MessageDigest.getInstance("md5")
                                         .digest(value)).toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] copy(byte[] bytes, int off, int len) {
    byte[] buf = new byte[len];
    System.arraycopy(bytes, off, buf, 0, len);
    return buf;
  }

  public static void copy(InputStream input, OutputStream output) throws IOException {
    byte[] buf = new byte[1024];
    int num;
    while ((num = input.read(buf)) != -1) {
      output.write(buf, 0, num);
    }
  }

}
