package pack.s3;

public class CRC64 {

  private static final long poly = 0xC96C5795D7870F42L;
  private static final long crcTable[] = new long[256];

  private long _crc = -1;

  static {
    for (int b = 0; b < crcTable.length; ++b) {
      long r = b;
      for (int i = 0; i < 8; ++i) {
        if ((r & 1) == 1)
          r = (r >>> 1) ^ poly;
        else
          r >>>= 1;
      }
      crcTable[b] = r;
    }
  }

  private CRC64() {

  }

  private CRC64(long crc) {
    _crc = crc;
  }

  public static CRC64 newInstance() {
    return new CRC64();
  }

  public static CRC64 newInstance(long crc) {
    return new CRC64(crc);
  }

  public long update(long val) {
    long crc = _crc;
    crc = crcTable[((byte) (val) ^ (int) crc) & 0xFF] ^ (crc >>> 8);
    crc = crcTable[((byte) (val >>> 8) ^ (int) crc) & 0xFF] ^ (crc >>> 8);
    crc = crcTable[((byte) (val >>> 16) ^ (int) crc) & 0xFF] ^ (crc >>> 8);
    crc = crcTable[((byte) (val >>> 24) ^ (int) crc) & 0xFF] ^ (crc >>> 8);
    crc = crcTable[((byte) (val >>> 32) ^ (int) crc) & 0xFF] ^ (crc >>> 8);
    crc = crcTable[((byte) (val >>> 40) ^ (int) crc) & 0xFF] ^ (crc >>> 8);
    crc = crcTable[((byte) (val >>> 48) ^ (int) crc) & 0xFF] ^ (crc >>> 8);
    return _crc = crcTable[((byte) (val >>> 56) ^ (int) crc) & 0xFF] ^ (crc >>> 8);
  }

  public long update(byte[] buf) {
    return update(buf, 0, buf.length);
  }

  public long update(byte[] buf, int off, int len) {
    long crc = _crc;
    int end = off + len;
    while (off < end) {
      crc = crcTable[(buf[off++] ^ (int) crc) & 0xFF] ^ (crc >>> 8);
    }
    return _crc = crc;
  }

}