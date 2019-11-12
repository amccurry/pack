package pack.rs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.UUID;

public class RSUtil {

  private static final String[] NATIVE_FILES = new String[] { "libhadoop.a", "libhadooppipes.a", "libhadoop.so",
      "libhadoop.so.1.0.0", "libhadooputils.a", "libhdfs.a", "libisal.a", "libisal.so", "libisal.so.2",
      "libisal.so.2.0.14", "libnativetask.a", "libnativetask.so", "libnativetask.so.1.0.0", "libsnappy.so",
      "libsnappy.so.1", "libsnappy.so.1.1.4", "libzstd.so", "libzstd.so.1" };

  public static void addDir(String s) throws IOException {
    try {
      // This enables the java.library.path to be modified at runtime
      // From a Sun engineer at
      // http://forums.sun.com/thread.jspa?threadID=707176
      //
      Field field = ClassLoader.class.getDeclaredField("usr_paths");
      field.setAccessible(true);
      String[] paths = (String[]) field.get(null);
      for (int i = 0; i < paths.length; i++) {
        if (s.equals(paths[i])) {
          return;
        }
      }
      String[] tmp = new String[paths.length + 1];
      System.arraycopy(paths, 0, tmp, 0, paths.length);
      tmp[paths.length] = s;
      field.set(null, tmp);
      System.setProperty("java.library.path", System.getProperty("java.library.path") + File.pathSeparator + s);
    } catch (IllegalAccessException e) {
      throw new IOException("Failed to get permissions to set library path");
    } catch (NoSuchFieldException e) {
      throw new IOException("Failed to get field handle to set library path");
    }
  }

  public static void copy(InputStream input, OutputStream output) throws IOException {
    byte[] buffer = new byte[1024];
    int read;
    while ((read = input.read(buffer)) >= 0) {
      output.write(buffer, 0, read);
    }
  }

  public static void loadNativeLibrary() {
    File dir = new File("/tmp/pack-native-" + UUID.randomUUID()
                                                  .toString());
    dir.mkdirs();
    dir.deleteOnExit();
    for (String filename : NATIVE_FILES) {
      try (InputStream input = RSManager.class.getResourceAsStream("/native/" + filename)) {
        File file = new File(dir, filename);
        file.deleteOnExit();
        try (OutputStream output = new FileOutputStream(file)) {
          RSUtil.copy(input, output);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      RSUtil.addDir(dir.getCanonicalPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void print(ByteBuffer[] outputParts) {
    for (ByteBuffer buffer : outputParts) {
      print(buffer);
    }
  }

  public static void print(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    while (duplicate.hasRemaining()) {
      System.out.print(duplicate.get() + " ");
    }
    System.out.println();
  }
}
