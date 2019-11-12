package pack.rs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import pack.rs.RSProcessor;

public class RS {

  private static final String PATH_SEPARATOR = "path.separator";
  private static final String JAVA_CLASS_PATH = "java.class.path";
  private static final String TMP_PACK_RS_PACKAGE = "/tmp/pack-rs-package-";
  private static final String NOTHING = "";
  private static final String PACK_RS_LIB = "pack-rs-lib/";
  private static final String CREATE = "create";
  private static final String PACK_RS_RS_MANAGER = "pack.rs.RSManager";
  private static final String PACK_RS_PACKAGE = "pack-rs-package";

  private static boolean SETUP = false;
  private static FileClassLoader CLASSLOADER;

  public static RSProcessor create(int byteBufferSize, int dataPartCount, int parityPartCount, int blockSize)
      throws IOException {
    String classPath = System.getProperty(JAVA_CLASS_PATH);
    String[] classPathElements = classPath.split(System.getProperty(PATH_SEPARATOR));
    for (String path : classPathElements) {
      if (path.contains(PACK_RS_PACKAGE)) {
        return create(path, byteBufferSize, dataPartCount, parityPartCount, blockSize);
      }
    }
    throw new RuntimeException(PACK_RS_PACKAGE + " jar not found");
  }

  private static RSProcessor create(String path, int byteBufferSize, int dataPartCount, int parityPartCount,
      int blockSize) throws IOException {
    FileClassLoader classLoader = initClassLoader(path);
    Thread thread = Thread.currentThread();
    ClassLoader currentContextClassLoader = thread.getContextClassLoader();
    try {
      thread.setContextClassLoader(classLoader);
      Class<?> clazz = classLoader.loadClass(PACK_RS_RS_MANAGER);
      Method method = clazz.getMethod(CREATE, new Class[] { Integer.TYPE, Integer.TYPE, Integer.TYPE, Integer.TYPE });
      return (RSProcessor) method.invoke(null,
          new Object[] { byteBufferSize, dataPartCount, parityPartCount, blockSize });
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(PACK_RS_RS_MANAGER + " class not found");
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      thread.setContextClassLoader(currentContextClassLoader);
    }
  }

  private synchronized static FileClassLoader initClassLoader(String path)
      throws IOException, FileNotFoundException, ZipException {
    if (SETUP) {
      return CLASSLOADER;
    }
    try {
      File dir = new File(TMP_PACK_RS_PACKAGE + UUID.randomUUID());
      dir.mkdirs();
      dir.deleteOnExit();
      byte[] buffer = new byte[4096];
      List<File> files = new ArrayList<File>();
      try (ZipFile zipFile = new ZipFile(new File(path))) {
        Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
        while (enumeration.hasMoreElements()) {
          ZipEntry entry = enumeration.nextElement();
          String name = entry.getName();

          if (name.startsWith(PACK_RS_LIB) && !entry.isDirectory() && isAllowed(name)) {
            try (InputStream inputStream = zipFile.getInputStream(entry)) {
              File file = new File(dir, name.replace(PACK_RS_LIB, NOTHING));
              file.deleteOnExit();
              files.add(file);
              try (OutputStream output = new FileOutputStream(file)) {
                int read;
                while ((read = inputStream.read(buffer)) != -1) {
                  output.write(buffer, 0, read);
                }
              }
            }
          }
        }
      }
      return CLASSLOADER = new FileClassLoader(files);
    } finally {
      SETUP = true;
    }
  }

  private static boolean isAllowed(String name) {
    if (name.contains("slf4j")) {
      return false;
    } else if (name.contains("log4j")) {
      return false;
    } else {
      return true;
    }
  }

}
