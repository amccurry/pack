package pack.iscsi.io.direct;

import java.io.File;
import java.io.IOException;

public class DirectIOSparseRead {
  public static void main(String[] args) throws IOException {
    File file = new File("./testfile");
    file.delete();
    try (DirectIO io = new DirectIO(file)) {
      io.setLength(1024L * 1024L);
      byte[] buffer = new byte[1024];
      io.read(0, buffer);
      for (byte b : buffer) {
        System.out.println(b);
      }
    }
  }
}
