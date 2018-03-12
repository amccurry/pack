package pack.distributed.storage.read;

import java.nio.ByteBuffer;

public class ReadRequest implements Comparable<ReadRequest> {

  private final int _blockId;
  private final ByteBuffer _dest;
  private final int _blockOffset;
  private boolean _empty;

  public ReadRequest(int blockId, int blockOffset, ByteBuffer dest) {
    _blockId = blockId;
    _blockOffset = blockOffset;
    _dest = dest;
  }

  public int getBlockOffset() {
    return _blockOffset;
  }

  public boolean isEmpty() {
    return _empty;
  }

  public boolean isCompleted() {
    return _dest.remaining() == 0;
  }

  public int getBlockId() {
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
    while (_dest.remaining() > 0) {
      _dest.put((byte) 0);
    }
    _empty = true;
  }

  @Override
  public int compareTo(ReadRequest o) {
    return Long.compare(_blockId, o._blockId);
  }

  @Override
  public String toString() {
    return "ReadRequest [completed=" + isCompleted() + ", blockId=" + _blockId + ", dest=" + _dest + ", blockOffset="
        + _blockOffset + ", empty=" + _empty + "]";
  }

}
