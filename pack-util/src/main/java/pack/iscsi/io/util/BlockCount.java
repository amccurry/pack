package pack.iscsi.io.util;

import java.io.File;

import pack.iscsi.io.IOUtils;

public class BlockCount {
  public static long getBlockCount(File file) throws Exception {
    String s = IOUtils.exec("stat", "-c", "%b", file.getAbsolutePath());
    return Long.parseLong(s.trim());
  }
}
