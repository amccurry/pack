package pack.nativehdfs;

import java.io.File;
import java.io.IOException;

public class NativeHdfs {

  public static void main(String[] args) throws IOException {
    File mnt = new File("/mnt/testing/mnt");
    mnt.mkdirs();
    File mirror = new File("/mnt/testing/mirror");
    mirror.mkdirs();
    try (NativeFuse fuse = new NativeFuse(mnt, mirror)) {
      fuse.localMount(true);
    }
  }

}
