package pack.nativehdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class NativeHdfs {

  public static void main(String[] args) throws IOException {
    Configuration configuration = new Configuration();
    Path path = new Path("/");
    new File("/test").mkdirs();
    try (NativeFuse fuse = new NativeFuse("/test", configuration, path)) {
      fuse.localMount(true);
    }
  }

}
