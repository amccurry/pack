package pack.block.blockstore.hdfs.file;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.crc.CrcLayer;

public class ReadRequest implements Comparable<ReadRequest> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadRequest.class);

  private final long _blockId;
  private final ByteBuffer _dest;
  private final int _blockOffset;
  private final CrcLayer _crcLayer;
  private boolean _empty;

  public ReadRequest(long blockId, int blockOffset, ByteBuffer dest) {
    this(blockId, blockOffset, dest, null);
  }

  public ReadRequest(long blockId, int blockOffset, ByteBuffer dest, CrcLayer crcLayer) {
    _blockId = blockId;
    _blockOffset = blockOffset;
    _dest = dest;
    _crcLayer = crcLayer;
  }

  public boolean isEmpty() {
    return _empty;
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
    validate("handleResult ByteBuffer", src);
    src.position(_blockOffset);
    src.limit(_blockOffset + _dest.remaining());
    _dest.put(src);
  }

  public void handleResult(byte[] src) {
    validate("handleResult src " + src.length, src, 0);
    _dest.put(src, _blockOffset, _dest.remaining());
  }

  public void handleResult(byte[] src, int offset) {
    validate("handleResult src " + src.length + " " + offset, src, offset);
    _dest.put(src, _blockOffset + offset, _dest.remaining());
  }

  public void handleEmptyResult() {
    _dest.position(_dest.remaining());
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

  private void validate(Object msg, ByteBuffer src) {
    if (_crcLayer == null) {
      return;
    }
    ByteBuffer duplicate = src.duplicate();
    if (duplicate.remaining() < _crcLayer.getBlockSize()) {
      LOGGER.warn("Can not validate crc with parital block {}", _blockId);
      return;
    }
    byte[] buf = new byte[_crcLayer.getBlockSize()];
    duplicate.get(buf);
    validate(msg, buf, 0);
  }

  private void validate(Object msg, byte[] src, int offset) {
    if (_crcLayer == null) {
      return;
    }
    _crcLayer.validate(msg, (int) _blockId, src, offset, _crcLayer.getBlockSize());
  }

}
