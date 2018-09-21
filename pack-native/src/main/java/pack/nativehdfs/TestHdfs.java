package pack.nativehdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class TestHdfs {

  public static void main(String[] args) throws IOException {
    byte[] buf1 = new byte[4];
    putInt(buf1, 0, 0755);

    int int1 = getInt(buf1, 0);
    System.out.println(int1);

    byte[] buf2 = new byte[4];
    putInt(buf2, 0, 755);

    String octalString = Integer.toOctalString(493);
    System.out.println(octalString);

    System.out.println((7 * Math.pow(8, 2)) + (5 * Math.pow(8, 1)) + (5 * Math.pow(8, 0)));
  }

  static void putInt(byte[] b, int off, int val) {
    b[off + 3] = (byte) (val);
    b[off + 2] = (byte) (val >>> 8);
    b[off + 1] = (byte) (val >>> 16);
    b[off] = (byte) (val >>> 24);
  }

  static int getInt(byte[] b, int off) {
    return ((b[off + 3] & 0xFF)) + ((b[off + 2] & 0xFF) << 8) + ((b[off + 1] & 0xFF) << 16) + ((b[off]) << 24);
  }
  
  

}
