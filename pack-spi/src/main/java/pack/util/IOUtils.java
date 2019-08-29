package pack.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

  public static void close(Logger logger, List<? extends Closeable> closeables) {
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

}
