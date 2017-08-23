package pack.block.util;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;

public class Utils {

  public static void closeQuietly(final Closeable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (final IOException ioe) {
      // ignore
    }
  }

  public static void close(final Logger logger, final Closeable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (final IOException ioe) {
      logger.error("Unknown error while trying to close.", ioe);
    }
  }
}
