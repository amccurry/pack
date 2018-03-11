package pack.distributed.storage.kafka.util;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class HeaderUtil {

  private static final String TID = "tid";

  public static Header toTransHeader(long transId) {
    return new Header() {

      @Override
      public String key() {
        return TID;
      }

      @Override
      public byte[] value() {
        return toBytes(transId);
      }
    };
  }

  public static long getTransId(Headers headers) {
    for (Header header : headers.headers(TID)) {
      return toLong(header.value());
    }
    return -1;
  }

  public static byte[] toBytes(long l) {
    byte[] buff = new byte[8];
    buff[7] = (byte) (l);
    buff[6] = (byte) (l >>> 8);
    buff[5] = (byte) (l >>> 16);
    buff[4] = (byte) (l >>> 24);
    buff[3] = (byte) (l >>> 32);
    buff[2] = (byte) (l >>> 40);
    buff[1] = (byte) (l >>> 48);
    buff[0] = (byte) (l >>> 56);
    return buff;
  }

  public static long toLong(byte[] buf) {
    return ((buf[7] & 0xFFL)) + ((buf[6] & 0xFFL) << 8) + ((buf[5] & 0xFFL) << 16) + ((buf[4] & 0xFFL) << 24)
        + ((buf[3] & 0xFFL) << 32) + ((buf[2] & 0xFFL) << 40) + ((buf[1] & 0xFFL) << 48) + (((long) buf[0]) << 56);
  }
}
