package pack.block.blockstore.hdfs.file;

import java.nio.ByteBuffer;

public class ReadRequest {

  private final long _blockId;
  private final ByteBuffer _dest;
  private final int _blockOffset;

  public ReadRequest(long blockId, int blockOffset, ByteBuffer dest) {
    _blockId = blockId;
    _blockOffset = blockOffset;
    _dest = dest;
  }

  public boolean isCompleted() {
    return _dest.remaining() == 0;
  }

  public long getBlockId() {
    return _blockId;
  }

  public ByteBuffer getByteBuffer() {
    return _dest;
  }

  public void handleResult(ByteBuffer src) {
    src.position(_blockOffset);
    src.limit(_blockOffset + _dest.remaining());
    _dest.put(src);
  }

  public void handleResult(byte[] src) {
    _dest.put(src, _blockOffset, _dest.remaining());
  }
  
  public void handleResult(byte[] src, int offset) {
    _dest.put(src, _blockOffset + offset, _dest.remaining());
  }

  public void handleEmptyResult() {
    _dest.position(_dest.remaining());
  }

}
