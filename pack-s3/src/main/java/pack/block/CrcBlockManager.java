package pack.block;

import java.io.Closeable;

public interface CrcBlockManager extends Closeable {

  public static CrcBlockManager create(Block block) {
    return new CrcBlockManager() {

      @Override
      public void sync() {
        block.sync();
      }

      @Override
      public void putBlockCrc(long blockId, long crc) throws Exception {
        byte[] buf = new byte[8];
        putLong(buf, 0, crc);
        long position = blockId * 8;
        block.writeFully(position, buf, 0, buf.length);
      }

      @Override
      public long getBlockCrc(long blockId) throws Exception {
        byte[] buf = new byte[8];
        long position = blockId * 8;
        block.readFully(position, buf, 0, buf.length);
        return getLong(buf, 0);
      }

      @Override
      public void close() {
        block.close();
      }

      private long getLong(byte[] b, int off) {
        return ((b[off + 7] & 0xFFL)) + ((b[off + 6] & 0xFFL) << 8) + ((b[off + 5] & 0xFFL) << 16)
            + ((b[off + 4] & 0xFFL) << 24) + ((b[off + 3] & 0xFFL) << 32) + ((b[off + 2] & 0xFFL) << 40)
            + ((b[off + 1] & 0xFFL) << 48) + (((long) b[off]) << 56);
      }

      private void putLong(byte[] b, int off, long val) {
        b[off + 7] = (byte) (val);
        b[off + 6] = (byte) (val >>> 8);
        b[off + 5] = (byte) (val >>> 16);
        b[off + 4] = (byte) (val >>> 24);
        b[off + 3] = (byte) (val >>> 32);
        b[off + 2] = (byte) (val >>> 40);
        b[off + 1] = (byte) (val >>> 48);
        b[off] = (byte) (val >>> 56);
      }
    };
  }

  long getBlockCrc(long blockId) throws Exception;

  void putBlockCrc(long blockId, long crc) throws Exception;

  void sync();

  @Override
  void close();

}
