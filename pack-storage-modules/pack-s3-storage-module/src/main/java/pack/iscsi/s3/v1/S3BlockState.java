package pack.iscsi.s3.v1;

import java.nio.ByteBuffer;

public enum S3BlockState {
  MISSING((byte) 0), CLEAN((byte) 1), DIRTY((byte) 2);

  private final byte[] _type;

  private S3BlockState(byte type) {
    _type = new byte[] { type };
  }

  public byte getType() {
    return _type[0];
  }

  public static S3BlockState lookup(byte type) {
    switch (type) {
    case 0:
      return MISSING;
    case 1:
      return CLEAN;
    case 2:
      return DIRTY;
    default:
      throw new RuntimeException("Unknown type " + type);
    }
  }

  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(_type);
  }

}
