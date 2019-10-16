package pack.iscsi.io;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IOUtils {

  private static final String BUFFER_PREFIX = "000000000000";
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

  public static void ls(Logger logger, File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      logger.info("dir {}", file.getAbsolutePath());
      for (File f : file.listFiles()) {
        ls(logger, f);
      }
    } else {
      logger.info("file {}", file.getAbsolutePath());
    }
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

  public static void copy(File src, File dst) throws IOException {
    try (FileInputStream input = new FileInputStream(src)) {
      try (FileOutputStream output = new FileOutputStream(dst)) {
        byte[] buffer = new byte[4096];
        int read;
        while ((read = input.read(buffer)) != -1) {
          output.write(buffer, 0, read);
        }
      }
    }
  }

  public static void close(Logger logger, Collection<? extends Closeable> closeables) {
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

  public static void close(Logger logger, ExecutorService... services) {
    if (services == null) {
      return;
    }
    for (ExecutorService service : services) {
      if (service != null) {
        try {
          service.shutdown();
          service.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          if (logger != null) {
            LOGGER.error("Unknown error while trying to close " + service, e);
          }
        }
      }
    }
  }

  public static String toStringWithLeadingZeros(long l) {
    String str = Long.toString(l);
    return new StringBuilder().append(BUFFER_PREFIX, 0, BUFFER_PREFIX.length() - str.length())
                              .append(str)
                              .toString();
  }

  public static String exec(String... args) throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder(args);
    Process process = builder.start();
    if (process.waitFor() == 0) {
      InputStream inputStream = process.getInputStream();
      return toString(inputStream);
    }
    throw new IOException(toString(process.getErrorStream()));
  }

  private static String toString(InputStream inputStream) throws IOException {
    byte[] buffer = new byte[4096];
    int read;
    StringBuilder stringBuilder = new StringBuilder();
    while ((read = inputStream.read(buffer)) != -1) {
      stringBuilder.append(new String(buffer, 0, read));
    }
    return stringBuilder.toString();
  }

  public static byte[] toByteArray(InputStream input) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int read;
    while ((read = input.read(buffer)) != -1) {
      output.write(buffer, 0, read);
    }
    return output.toByteArray();
  }

  public static void close(Logger logger, Thread... threads) {
    if (threads == null) {
      return;
    }
    for (Thread t : threads) {
      Thread thread = t;
      if (t != null) {
        close(logger, () -> {
          thread.interrupt();
          try {
            thread.join();
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        });
      }
    }
  }

}
